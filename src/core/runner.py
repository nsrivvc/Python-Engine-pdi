"""
runner.py
=========
Orchestration. Runs one or all registered transformations, each in its own
transaction, with dependency checks, timing, logging, and error isolation.

A failure in one transformation is logged and does not stop the others; the
runner reports an overall non-zero result if anything failed (run.py turns that
into a non-zero exit code, which schedulers use to flag failed jobs).

A transformation is skipped entirely — no CREATE TABLE, no INSERT/UPDATE — once
its Silver table already exists. The table is only ever populated on the run
that creates it; rerunning after that is a deliberate no-op. Pass reload=True
(CLI: --reload) to drop and rebuild instead — the stage workflows all do, so
every scheduled or ingest-triggered run refreshes from current Bronze.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List

from ..db.connection import get_engine, table_exists
from ..logging_config import get_logger
from .base import PipelineTransformation
from .registry import REGISTRY
from .sources import normalize_source

# Importing the transformations package triggers auto-discovery (it imports
# every module in src/transformations/, each of which @register-s itself).
from .. import transformations  # noqa: F401  (side-effect import)

log = get_logger(__name__)


#: How a run touched its target table. This is the *write semantics* of the
#: transformation, which `status` alone does not carry: a succeeded run may
#: have appended to an archive, folded rows into a ledger it preserved, or
#: dropped and rebuilt the table from scratch.
#:
#:   APPENDED  - rows added to a raw archive; nothing is removed (stage 2's
#:               json_to_raw, which is a standalone loader and does not run
#:               through run_one -- the constant is here so consumers have one
#:               vocabulary for all four cases).
#:   PRESERVED - `incremental` target: never dropped, even on --reload. New
#:               rows are folded in; 0 rows means nothing changed.
#:   REBUILT   - dropped and recreated this run, so the contents are
#:               re-materialized and BIGSERIAL ids restart at 1.
#:   SKIPPED   - no CREATE, no INSERT. A deliberate no-op.
WRITE_APPENDED = "appended"
WRITE_PRESERVED = "preserved"
WRITE_REBUILT = "rebuilt"
WRITE_SKIPPED = "skipped"


@dataclass
class Result:
    name: str
    status: str            # "succeeded" | "failed" | "skipped"
    rows: int = 0
    duration_s: float = 0.0
    error: str = ""
    #: One of the WRITE_* constants above - see run_one, which decides it in
    #: the same branch that decides whether to keep, drop or skip the table.
    write_mode: str = ""


def group_of(t: PipelineTransformation) -> str:
    """The folder path a transformation lives in, or "" for a top-level module.

    src.transformations.stage_4.rec_del_pairing.silver_firm_rec_del_pair
        -> "stage_4/rec_del_pairing"
    src.transformations.silver_firm_transport_rate
        -> ""
    """
    parts = type(t).__module__.split(".")
    try:
        rest = parts[parts.index("transformations") + 1:]
    except ValueError:  # pragma: no cover - transformation defined elsewhere
        return ""
    return "/".join(rest[:-1])


def _in_group(path: str, group: str) -> bool:
    """Whether a transformation's folder path is selected by `group`.

    Matching is deliberately loose so a stage, a component, or the full path all
    work as selectors, and so nesting a component under a stage later does not
    invalidate existing callers:

        "stage_4"                       -> everything in stage 4
        "rec_del_pairing"               -> just that component, wherever it sits
        "stage_4/rec_del_pairing"       -> the same, spelled out
        "master_capacity/firm/core"     -> one leaf, without naming its stage

    The last form is why this matches a *contiguous run* of segments rather than
    just a prefix: workflows address a leaf folder by its own path, and should
    not have to repeat the stage that happens to contain it today.
    """
    group = group.strip("/")
    if not group:
        return path == ""
    want = group.split("/")
    have = path.split("/")
    return any(have[i:i + len(want)] == want for i in range(len(have) - len(want) + 1))


def source_of(t: PipelineTransformation) -> str:
    """Canonical JSON source feed for a transformation ("firm", "ioc", ...).

    Anything cross-feed or undeclared resolves to "_combined".
    """
    return normalize_source(t.source)


def list_transformations(group: str | None = None, source: str | None = None) -> List[str]:
    """All registered names, narrowed by phase folder and/or JSON source feed.

    The two filters are independent and compose: group picks the stage or
    component, source picks the feed, so a workflow can run exactly one feed of
    one stage.
    """
    names = sorted(REGISTRY.keys())
    if group is not None:
        names = [n for n in names if _in_group(group_of(REGISTRY[n]), group)]
    if source is not None:
        wanted = normalize_source(source)
        names = [n for n in names if source_of(REGISTRY[n]) == wanted]
    return names


def list_sources() -> List[str]:
    """Source feeds that currently have at least one transformation."""
    return sorted({source_of(t) for t in REGISTRY.values()})


def list_groups() -> List[str]:
    """Folder paths that currently contain at least one transformation."""
    return sorted({g for g in (group_of(t) for t in REGISTRY.values()) if g})


def _check_dependencies(conn, t: PipelineTransformation) -> List[str]:
    """Return the list of missing source tables (empty == all present).

    Sources are looked up in `t.source_schema`, which is Bronze for a plain
    Bronze -> Silver transformation but a later stage for transformations that
    build on one (see PipelineTransformation.source_schema).
    """
    if not t.sources_required:
        return []   # transformation adapts to whichever sources exist
    missing = []
    for src in t.bronze_sources:
        if not table_exists(conn, t.source_schema, src):
            missing.append(f"{t.source_schema}.{src}")
    return missing


def _silver_table_exists(conn, t: PipelineTransformation) -> bool:
    """True if this transformation's target table has already been created."""
    return table_exists(conn, t.target_schema, t.table_name)


def run_one(name: str, reload: bool = False) -> Result:
    """Run a single transformation by name, in its own transaction."""
    if name not in REGISTRY:
        raise KeyError(f"Unknown transformation {name!r}. Known: {list_transformations()}")
    t = REGISTRY[name]
    engine = get_engine()
    start = time.perf_counter()

    log.info("[%s] starting (reads: %s)", name, ", ".join(t.bronze_sources) or "n/a")
    try:
        with engine.begin() as conn:  # commits on success, rolls back on exception
            missing = _check_dependencies(conn, t)
            if missing:
                msg = f"missing sources: {', '.join(missing)}"
                log.warning("[%s] skipped — %s", name, msg)
                return Result(name, "skipped", 0, time.perf_counter() - start, msg,
                              WRITE_SKIPPED)

            # A target that does not exist yet is materialized by this run, so
            # the default is REBUILT; the branches below correct it where the
            # table already exists. An incremental target is PRESERVED either
            # way -- on its very first run it is the ledger being opened, not a
            # derived view being recomputed.
            write_mode = WRITE_PRESERVED if t.incremental else WRITE_REBUILT
            if _silver_table_exists(conn, t):
                if t.incremental:
                    # The table is state the transformation owns across runs
                    # (e.g. the ammendments(p2) version history): never dropped
                    # on --reload, never load-once skipped. The run itself
                    # decides what is new and folds it in.
                    log.info("[%s] incremental — keeping %s.%s and folding in "
                             "new rows", name, t.target_schema, t.table_name)
                elif not reload:
                    msg = f"target table already exists: {t.target_schema}.{t.table_name}"
                    log.info("[%s] skipped — %s", name, msg)
                    return Result(name, "skipped", 0, time.perf_counter() - start, msg,
                                  WRITE_SKIPPED)
                else:
                    # --reload: drop and rebuild so the table refreshes from
                    # source. Inside the same transaction, so a failed rebuild
                    # leaves the existing table untouched.
                    log.warning("[%s] reload - dropping %s.%s", name, t.target_schema, t.table_name)
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {t.target_schema}.{t.table_name}")

            # Machine-readable restatement of the branch above, for log readers
            # that cannot import Result (the dashboard reads GitHub Actions job
            # logs). Additive: the lines above keep their exact wording.
            log.info("[%s] write_mode=%s target=%s.%s", name, write_mode,
                     t.target_schema, t.table_name)

            rows = t.run(conn)
        dur = time.perf_counter() - start
        log.info("[%s] succeeded — %s rows affected in %.2fs", name, rows, dur)
        return Result(name, "succeeded", rows, dur, "", write_mode)

    except Exception as exc:  # noqa: BLE001 — isolate and report per transformation
        dur = time.perf_counter() - start
        log.exception("[%s] FAILED after %.2fs: %s", name, dur, exc)
        return Result(name, "failed", 0, dur, f"{type(exc).__name__}: {exc}")


def run_all(reload: bool = False, source: str | None = None) -> List[Result]:
    """Run every registered transformation; continue past failures."""
    names = list_transformations(source=source)
    if source is not None and not names:
        log.warning(
            "No transformations for source %r - nothing to do. Known sources: %s",
            source, ", ".join(list_sources()) or "(none)",
        )
        return []
    results = [run_one(name, reload) for name in names]
    _summarize(results)
    return results


def run_group(group: str, reload: bool = False,
              source: str | None = None) -> List[Result]:
    """Run every transformation in one phase folder; continue past failures.

    An empty group is not an error — a phase folder that exists but holds no
    transformations yet (e.g. master_capacity) is a no-op, so scheduled jobs can
    reference it before the code lands without failing.
    """
    names = list_transformations(group, source)
    if not names:
        log.warning(
            "Group %r%s has no registered transformations - nothing to do. "
            "Known groups: %s | known sources: %s",
            group, f" / source {source!r}" if source else "",
            ", ".join(list_groups()) or "(none)",
            ", ".join(list_sources()) or "(none)",
        )
        return []
    log.info("Running group %r%s: %s", group,
             f" / source {source!r}" if source else "", ", ".join(names))
    results = [run_one(name, reload) for name in names]
    _summarize(results)
    return results


def _summarize(results: List[Result]) -> None:
    by_status: Dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1
    total_rows = sum(r.rows for r in results)
    log.info(
        "SUMMARY: %s | total rows affected: %s",
        ", ".join(f"{k}={v}" for k, v in sorted(by_status.items())),
        total_rows,
    )


def any_failed(results: List[Result]) -> bool:
    return any(r.status == "failed" for r in results)
