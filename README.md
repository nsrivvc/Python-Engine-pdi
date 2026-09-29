# Stages 1 - 5

A modular batch system that reads **Bronze** tables from a data source, applies
transformation logic, and writes curated **Silver** tables. This repo handles
**only** Bronze → Silver. Ingestion (JSON → Bronze) and export (Silver →
downstream) live elsewhere.

Each Silver table is its own self-registering module, so new transformations are
added by dropping in a file — no central list to edit.

## Pipeline stages

Transformations are grouped by stage, then by component. **Most of this is
scaffolding**: the folders and the runner wiring exist, the business logic does
not. Current state:

| Stage | Component | Status |
|---|---|---|
| — | `silver_firm_transport_rate` | **Implemented** — the worked example |
| 3 | `decompisition/` | Scaffolded, empty `logic.txt` |
| 3 | `standardization/` | Scaffolded, empty `logic.txt` |
| 3 | `deduplication/` | Scaffolded, empty `logic.txt` |
| 3 | `ammendments/` | Scaffolded, empty `logic.txt` |
| 4 | `rec_del_pairing/` | **Wired end-to-end**, two business rules pending |
| 5 | `master_capacity/<feed>/<grain>/` | Scaffolded (15 folders), empty `logic.txt` |
| 5 | `master_capacity/final/<grain>/` | **Wired end-to-end**, column models pending |

Stage 3 splits each feed into core / locations / rates per type, standardizes and
deduplicates it, and applies amendments. Stage 4 pairs receipts to deliveries.
Stage 5 assembles master capacity per feed, then ties every feed together into
the three FINAL tables.

> The stage-3 folders contain no `.py` files, so `find -name "*.py"` won't show
> them and git won't track the empty directories. They exist.

### What still needs business rules

- **Stage 3 in full.** Nothing is written. Rec-del pairing reads stage 3's
  locations tables, so those are the first thing worth filling in.
- **Rec-del pairing: one hook**, marked `SPEC:` in
  `src/transformations/stage_4/rec_del_pairing/pairing_base.py`:
  - `term_columns_sql()` — the term transform. The placeholder passes the raw
    window through and leaves `term_days` / `term_category` NULL.

  The pairing itself is in place: one row per receipt/delivery location, each
  carrying its contract's formation (`R-D`, `R-R-R-D-D-D`), the configured
  pattern that admitted it, a `PAIRED` / `FAIL` status, and a `pair_set_id`
  that restarts at 1 per contract and is shared by the locations that belong
  together. A contract with one receipt and one delivery is therefore two rows
  (set 1, 1); the pattern regex decides the set unit (`^(R-D)+$` gives
  1,1,2,2, anything else makes the whole formation one set). Contracts no
  pattern admits still appear, marked `FAIL`.
- **Master capacity in full.** The target model already exists as
  `public.final_core_master_capacity` (27 columns). Note it lives in `public`
  with PascalCase columns, unlike everything else this repo writes — worth
  deciding whether to target it as-is or a Silver-schema equivalent.

Only **firm** and **interruptible** have ingestion feeds today. The **awards**
and **IOC** pairing transformations are written but dormant: the runner reports
them as skipped until their source tables appear, then they start working with no
code change.

## Architecture

```
run.py                        CLI: --list / --table / --group / --all / --show-sql / --inspect
src/
  config.py                   env-driven settings (DB URL, schema names, log level)
  logging_config.py           logging setup (stdout)
  db/connection.py            SQLAlchemy engine factory (the only driver-aware file)
  core/
    base.py                   PipelineTransformation base class (the shared pattern)
    registry.py               @register decorator + REGISTRY
    runner.py                 run one / group / all: per-table transaction, checks, logging
    inspect.py                read-only Bronze/Silver snapshot + readiness
  transformations/
    __init__.py               auto-discovers every module, recursively
    silver_firm_transport_rate.py   implemented example
    stage_3/
      decompisition/ standardization/ deduplication/ ammendments/
    stage_4/
      rec_del_pairing/
        pairing_base.py       shared pairing logic + the two SPEC hooks
        silver_{firm,interruptible,awards,ioc}_rec_del_pair.py
    stage_5/
      master_capacity/
        {firm,interruptible,awards,ioc,index}/{core,locations,rates}/
                              per-feed assembly, one package per table
        final/
          final_base.py       shared UNION-across-feeds logic + SPEC hooks
          {core,locations,rates}/   the three FINAL tables
```

Folders are named `stage_3` / `stage_4` / `stage_5`, not `stage 3`, because they
are imported as Python packages.

- **One file per Silver table.** Each subclasses `PipelineTransformation` and
  provides `table_name`, `create_table_sql()`, and `transform_sql()`.
- **Per-table transactions.** The runner runs each transformation in its own
  `engine.begin()` block, so a failure rolls back cleanly and doesn't stop the
  others. It also checks the required source tables exist first (skips with a
  clear message if not).
- **Sources aren't always Bronze.** A transformation declares `source_schema`;
  it defaults to Bronze, and rec-del pairing overrides it to read the
  decomposition phase's output instead. `--inspect` follows this, so readiness
  is reported against the right schema.
- **Load-once, then skip.** Before doing anything, the runner checks whether
  `silver_schema.table_name` already exists. If it does, the transformation is
  skipped entirely — no `CREATE TABLE`, no `INSERT`/`UPDATE`. A table is only
  ever populated on the run that creates it; reruns after that are a deliberate
  no-op, even if the underlying Bronze rows have changed. The SQL itself is
  still written idempotently (`CREATE TABLE IF NOT EXISTS` + a `UNIQUE` natural
  key + `ON CONFLICT DO UPDATE`) so a single first run can't produce duplicates,
  but that's the only protection — see [Reloading a table](#reloading-a-table)
  to force a refresh.
- **Driver-isolated.** Only `db/connection.py` imports the driver, so
  `--list` / `--show-sql` work with no database, and swapping databases later is
  contained to one file.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env          # then edit DATABASE_URL to point at your Postgres
```

Schema names are all env-driven: `BRONZE_SCHEMA` (default `bronze`),
`SILVER_SCHEMA` (default `silver`), and `DECOMP_SCHEMA` (default
`silver_staging`) — the last is where rec-del pairing looks for the decomposition
phase's output, and must match wherever that phase actually writes.

For a local/dummy Postgres, the quickest option is Docker:

```bash
docker run --name pa-postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pipeline -p 5432:5432 -d postgres:16
```

(That matches the default `DATABASE_URL` in `.env.example`.) This codebase
assumes the **Bronze tables already exist**; it reads them and writes Silver.

> Rec-del pairing uses `UNIQUE NULLS NOT DISTINCT`, which requires **Postgres
> 15+**, so that image tag is a floor for running stage 4 locally.

## Run

```bash
python run.py --list                                 # what's registered
python run.py --list-groups                          # what's registered, by folder
python run.py --inspect                              # snapshot tables + row counts (no transforms)
python run.py --show-sql silver_awards_core          # inspect SQL (no DB needed)
python run.py --table silver_awards_core             # run one transformation
python run.py --group rec_del_pairing                # run one component
python run.py --group stage_4                        # run a whole stage
python run.py --group master_capacity/firm/core      # run one leaf folder
python run.py --source firm                          # run one source feed
python run.py --all                                  # run all of them
python run.py --all --reload                         # rebuild tables that already exist
```

`--group` matches on any folder segment, so a component can be selected by its
own name (`rec_del_pairing`), by its stage (`stage_4`), or by the full path
(`stage_4/rec_del_pairing`). A folder that exists but holds no transformations
yet logs a warning and exits 0, so scheduled jobs can reference it before the
code lands.

Run `--inspect` before transforming to confirm which source tables exist and how
many rows they hold, and whether each transformation is READY (will run),
BLOCKED (missing a source), or SKIP (its Silver table already exists, so the run
would be a no-op). It's read-only — it never writes or alters data — so it's safe
to run anytime to check the state as data flows through. Run it again afterward
to see the new Silver row counts.

The run commands exit non-zero if any transformation fails, so a scheduler can
flag the job.

## Add a new Silver table

1. Copy `src/transformations/silver_firm_transport_rate.py` into the folder for
   its stage/component, e.g. `stage_5/master_capacity/final/core/`.
2. Change five things (each is commented in the example):
   - the class name and `name` (registry key / CLI name)
   - `table_name` (the bare table name created in the Silver schema — the
     runner checks for exactly this table to decide whether to skip the run)
   - `bronze_sources` (the tables you read)
   - `create_table_sql()` (your columns/types — must create `table_name`)
   - `transform_sql()` (your column mapping + business rules)
3. If it reads something other than Bronze, override `source_schema`.
4. Keep the idempotency pieces: `CREATE TABLE IF NOT EXISTS` with a `UNIQUE`
   natural key, and `INSERT … SELECT … ON CONFLICT (key) DO UPDATE`. They
   protect a single first run from producing duplicates; they do not cause
   later reruns to refresh anything, since the runner skips once the table
   exists.

That's it — `python run.py --list` will show it immediately. Discovery is
recursive, so no imports or registration boilerplate are needed in any folder.

> Most transformations are pure SQL. If one needs real Python logic, override
> `run(self, conn)` in your subclass instead of using the two SQL methods.

> For a family of near-identical transformations (one per transport type), put
> the shared SQL in a base class and keep the subclasses to just names — see
> `stage_4/rec_del_pairing/` for the pattern.

## Reloading a table

Because a transformation is skipped once its Silver table exists, picking up
new or corrected Bronze rows means dropping the table first:

```sql
DROP TABLE silver.firm_transport_rate;
```

Then rerun it:

```bash
python run.py --table silver_firm_transport_rate
```

`--inspect` will show `SKIP` for any transformation whose table is already
present — that's the signal a drop is needed before it will load again.

> This bites hardest while iterating on unfinished business rules: once a table
> is created, editing its SQL changes nothing until you drop it.

**Stage 5 model changes.** The per-feed master capacity tables are built from
the shared model in `src/transformations/stage_5/master_capacity/models.py`.
Changing that model (adding a column, changing a natural key) does not touch a
per-feed table that already exists: it is only rebuilt when its own feed's
stage 5 workflow reruns, which passes `--reload`. The FINAL tables check each
per-feed table against the model before UNIONing it. A stale table that is
**empty** (a feed that was not selected, or has no Bronze rows) is left out
with a warning, so the final holds just the feeds that have data. A stale
table that **holds rows** fails the final, naming the table, the missing
column(s) and the feed to rerun, rather than silently publishing a final
without that feed. Rerun the named feed's `*(stage3_4_5).yml` chain and
`finals(stage5).yml` goes through on the next trigger. Only the
`*(stage3_4_5).yml` orchestrators trigger `finals(stage5).yml` automatically;
after a standalone per-grain stage 5 run, dispatch the finals by hand.

## Shipper scoping

The dashboard's **Shippers** panel attaches shippers to a workflow as
KHolderName + KHolderNumber pairs, each with an **Add** or **Remove** action.
Those rows land in `<BRONZE_SCHEMA>.shipper_mapping`, and the pipeline filters
Bronze down to them.

| column | meaning |
| --- | --- |
| `workflow_id` | which dashboard workflow attached it — display/audit only |
| `source` | feed this row scopes (`firm`, `interruptible`, `awards`, `ioc`, `index`); `NULL` = every feed |
| `kholdernumber` | the DUNS matched against the feed |
| `kholdername` | display name as entered |
| `action` | `add` or `remove` |

The rule, per feed:

* **no `add` rows** → unscoped, everything passes (the behaviour before anyone
  configures a shipper — this feature is inert until a row exists)
* **one or more `add` rows** → only those DUNS are processed
* **any `remove` row** → that DUNS never passes, even if also added

**Where it is applied.** `Deduplication` in
`deduplication(p1)` are the only two classes that read a Bronze table — p2, p3,
stage 4 and stage 5 all read the staging schema those two write. The filter is a
`WHERE` clause inside their `INSERT`, so it scopes every feed and every grain
(core / locations / rates) at once, and every later phase inherits the scope
without knowing scoping exists. See `src/core/shipper_scope.py`.

Each feed spells the shipper differently, so the join column comes from
`SHIPPER_KEYS` rather than being hardcoded:

| Bronze table | feed | DUNS column |
| --- | --- | --- |
| `gtran_firm` | firm | `kholder` |
| `gtran_it` | interruptible | `kholder` |
| `gawd` | awards | `bidderduns` |
| `gindex` | index | `shipperduns` |

A Bronze table missing from that map is **not** filtered, and logs a warning — a
feed with no shipper concept should not silently lose every row. Adding a feed
means adding one line there and nothing else.

Inspect the live scope with:

```bash
python run.py --shippers                  # everything configured
python run.py --shippers --source firm    # just one feed
```

**Applying a change.** A shipper row only changes config; the feed keeps
whatever scope it was last built with until it is rebuilt. `shipper_scope_apply.yml`
is the trigger — the dashboard posts a `shipper-scope-changed`
`repository_dispatch` naming the selected feeds, and it replays those feeds'
existing `*(stage3_4_5).yml` chains. Those run with `--reload`, so the rebuild
re-evaluates the mapping table and lands the new scope with no extra logic:

```
POST /repos/<owner>/<repo>/dispatches
{ "event_type": "shipper-scope-changed",
  "client_payload": { "sources": "firm,interruptible" } }
```

Any number of feeds can be named; each has its own gated job. Omitting
`sources` replays all of them.

## Running it in CI

`run.py` is a plain batch command that reads all config from environment
variables, which makes it portable to any runner without code changes.

### The FINAL tables run once, after the feeds

The three cross-feed FINAL tables UNION whichever per-feed master capacity
tables exist, so they must run **once, after every feed has finished** — never
once per feed.

They used to run at the end of each `<feed>(stage3_4_5).yml`. Fine for one feed;
broken for several. The FINAL workflows are guarded by the concurrency groups
`stage5-final-core`, `-locations` and `-rates`, and GitHub keeps at most **one
running plus one pending** request per group, so a third arrival evicts the
queued one:

```
Canceling since a higher priority waiting request for stage5-final-rates exists
```

That cancelled job belonged to a feed's own run, so it cancelled that feed's
entire run — exactly one feed, chosen by a race, which is why a three-source run
failed a different source every time.

Now the finals reach the same place by two paths, and neither can evict a feed:

| How the feeds are triggered | Where the finals come from |
|---|---|
| Direct dispatch of `<feed>(stage3_4_5).yml` (one or several) | `finals(stage5).yml`, via `workflow_run` on completion |
| `all_sources(stage1-5).yml` / `shipper_scope_apply.yml` | their own `final_*` jobs, after their feed jobs |

`finals(stage5).yml` holds a single `stage5-finals` concurrency group with
`cancel-in-progress: true`. Several feeds finishing together produce several runs
of it, and the newest supersedes the rest — but that supersession happens between
runs of *that* workflow, which no feed's run depends on. The survivor is the one
triggered by the **last** feed to finish, so it consolidates after every feed's
stage 5 is done.

The two orchestrators call the feed workflows as **reusable workflows**, which
emit no `workflow_run` event, so they don't double up.

This also closes a correctness hazard the cancellations were masking: whichever
finals call won the old race was not necessarily the last one, so it could
consolidate while another feed's stage 5 was still running — publishing stale or
missing rows and exiting green.

**Multi-source runs** can still be fired as a single dispatch, which gives one
run whose status covers everything:

```
POST /repos/<owner>/<repo>/dispatches
{ "event_type": "run-pipeline",
  "client_payload": { "sources": "firm,interruptible,awards" } }
```

That is tidier, but no longer required — firing the three feed workflows
separately now consolidates correctly too.

**29 workflows.** Stages 3 and 4 split by source feed; stage 5 splits by feed
*and* grain, one file per table. Each runs, logs, fails and uploads independently.

| Stage | Files | Count | Runs |
|---|---|---|---|
| 3 | `(stage3)bronze_to_silver_<feed>.yml` | 4 | `--all --source <feed>` |
| 4 | `(stage4)rec_del_pairing_<feed>.yml` | 4 | `--group rec_del_pairing --source <feed>` |
| 5 | `(stage5)master_capacity_<group>_<grain>.yml` | 18 | `--group master_capacity/<group>/<grain>` |

Stage 5's 18 = 6 groups (`firm`, `interruptible`, `awards`, `ioc`, `index`,
`final`) × 3 grains (`core`, `locations`, `rates`). Each targets one folder
under `src/transformations/stage_5/master_capacity/`, so whatever is registered
there is what runs — no transformation names are hardcoded in any workflow, and
a folder with no code yet is a logged no-op that exits 0.

**All are manual only — there are no cron schedules, and nothing enforces
ordering.** Run them by hand:

1. Stage 3 for each feed (these are independent — they can run together).
2. Stage 4 for each feed, once that feed's stage 3 has finished.
3. Stage 5 for each feed.
4. **`master_capacity_final` last.** The three FINAL tables aggregate across
   every feed, so running it before the four per-feed stage 5 workflows have
   finished produces finals over incomplete data.

To automate the chain later, add `workflow_run` triggers keyed on the upstream
workflow completing, rather than cron offsets which only approximate the
dependency.

Each workflow:

- runs on manual dispatch only;
- lets you pick `all` or a single transformation at dispatch time (a named
  transformation bypasses the feed filter, since the name already pins it);
- has a `concurrency` group scoped to its stage *and* feed, so feeds never block
  each other but a run never collides with itself;
- reads the Neon connection string from the `DATABASE_URL` repo secret (the same
  secret used by the ingestion repo — Settings → Secrets and variables →
  Actions), and fails fast with a clear error if it isn't set;
- runs `python run.py --inspect` before and after, so the run log shows inputs
  and the resulting Silver row counts.

The stage 4 and 5 workflows each have their own `concurrency` group, so a run
never collides with itself but the two don't block each other. They select work
by folder rather than by a hardcoded list, so modules added to those folders are
picked up with no workflow change.

`master-capacity` is safe to run now: with nothing registered in that folder it
logs a warning and exits 0.

To run any of them: push the repo, add the `DATABASE_URL` secret, then Actions →
pick the workflow → **Run workflow**.

### Stage 1: API -> JSON

Stage 1 is [`src/transformations/stage_1/`](src/transformations/stage_1/): the
mock NatGasHub API (`src/mock_api/`) plus the `data/` fixtures it serves. Its
whole job is API -> JSON — serve the feeds over HTTP, fetch one to a file. The
fetched file is stage 1's entire output; stage 2 takes it from there. Nothing
in it imports from the stage 2-5 code or vice versa, and its only dependencies
(fastapi + uvicorn) live in its own `requirements.txt`.

(Stage 1 used to be half of a merged `stage_1_2(ingestion)` subproject, whose
`main.py` duplicated the stage 2 loader. That subproject is retired: the mock
API and fixtures moved here, the Bronze DDL moved to `stage_2/sql/`, and the
`bronze_ingest_*.yml` workflows now call `stage_2/json_to_raw.py` directly.)

Five workflows drive the fetch-then-load pair (stage 1, then stage 2):

| workflow | feed | loads |
|---|---|---|
| `bronze_ingest_firm.yml` | firm | `bronze.gtran_firm` |
| `bronze_ingest_interruptibles.yml` | interruptible | `bronze.gtran_it` |
| `bronze_ingest_ioc.yml` | index of customers | `bronze.gindex` |
| `bronze_ingest_awards.yml` | capacity release awards | `bronze.gawd` |
| `bronze_ingest.yml` | all four in parallel | (fan-out) |

**Running the mock NatGasHub API locally** (stage 1 — serves the JSON feeds):

```powershell
cd "src\transformations\stage_1"
python -m uvicorn src.mock_api.app:app --host 127.0.0.1 --port 8000
```

Then `http://127.0.0.1:8000/docs` for the interactive endpoint list, or hit
`/api/firms`, `/api/interruptibles`, `/api/ioc`, `/api/awards` directly — each
returns the matching `data/*.json` fixture verbatim. Full instructions (CORS,
changing the served data, how CI starts it automatically) are in the
[stage 1 README](src/transformations/stage_1/README.md).

Because everything is one repo, the feed orchestrators
(`firm(stage3_4_5).yml`, `interruptible(stage3_4_5).yml`,
`awards(stage3_4_5).yml`) now begin with an `ingest` job that calls the
matching `bronze_ingest_*.yml` as a reusable workflow, then chain stage 3 ->
4 -> 5 -> finals off it. One `workflow_dispatch` of an orchestrator runs the
whole pipeline end to end; the old cross-repo `repository_dispatch` handoff
(`bronze-*-loaded`) is no longer emitted, though the orchestrators still
accept those events for backwards compatibility.

The FINAL tables always run at the end of the chain: they consolidate whatever
feeds currently sit in Silver, so a firm-only run rebuilds them from firm's
fresh rows plus whatever the other feeds last left behind.

### Stage 2 on its own: a JSON file -> its raw table

[`src/transformations/stage_2/`](src/transformations/stage_2/) is stage 2 with
nothing else attached — no API, no Silver logic, no deduplication
(that is stage 3's `deduplication(p1)` phase). Give it a JSON file and it
writes the records into the matching raw table. Five files, one per feed plus
the loader that drives them, and the Bronze DDL:

| file | holds |
|---|---|
| `json_to_raw.py` | the loader and its CLI — the only file with logic in it |
| `firm.py` | gTRAN_FIRM -> `bronze.gtran_firm` |
| `interruptible.py` | gTRAN_IT -> `bronze.gtran_it` |
| `awards.py` | gAWD -> `bronze.gawd` |
| `ioc.py` | gINDEX -> `bronze.gindex` |
| `sql/create_bronze_tables.sql` | the raw tables' DDL, applied by `--create-tables` |

A feed module is a short list of declarations, no code: the words that name it
in a file name, the envelope its records arrive under, which key is the record
id, which fields are required, and any field the export spells differently from
its column (`KQty` fills `kqtyk`). Adding a feed is one more file plus one line
in `FEEDS`.

**The file name picks the table.** `firms_test.json` goes to `gtran_firm`,
`interruptibles_*.json` to `gtran_it`, `awards_*.json` to `gawd`, `ioc_*.json`
to `gindex` — which is how stage 1 already names what it fetches
(`_fetched_firms.json`). There is nothing else to pass. Matching is on whole
words, so `capacity_awards.json` is awards and not also interruptible; a name
that says nothing, or names two feeds, is refused rather than guessed at.

Run it with [`(stage2)json_to_raw.yml`](.github/workflows/(stage2)json_to_raw.yml)
(inputs: `file`, `dry_run`) or directly:

```powershell
python "src\transformations\stage_2\json_to_raw.py" --file "src\transformations\stage_1\data\firm_sample_worklow.json"
python "src\transformations\stage_2\json_to_raw.py" --file <path> --dry-run
python "src\transformations\stage_2\json_to_raw.py" --create-tables   # DDL only
```

**The database owns the schema.** The loader reads the target table's columns
from `information_schema` instead of declaring them, so it cannot drift from
what is actually in Neon: a column added there is populated on the next run,
and a JSON key with nowhere to land is reported rather than dropped (it is
still kept in full in `raw_payload`). It needs the raw table to exist — pass
`--create-tables` to apply `sql/create_bronze_tables.sql` first (all
CREATE/ALTER IF NOT EXISTS, so it is safe on every run), or run the flag on
its own to just create the tables.

Bronze keeps every load, duplicates included. Stage 3's deduplication(p1)
is the one place duplicate rows are dropped, by comparing the rows' own data
fields — nothing in stages 1-2 takes part in that decision.

This overlaps `bronze_ingest_*.yml` on purpose. Those run stage 1 **and** 2
together (fetch from the mock API, then load); this one is the load half by
itself, for when the JSON already exists.

For schedulers:

- **Azure Container Apps job** — build a small image (`python:3.11-slim`,
  `pip install -r requirements.txt`, `CMD ["python","run.py","--all"]`) and set
  `DATABASE_URL` as a secret/env var on the job.

Because nothing is hardcoded and logs go to stdout, the same command works
locally, in CI, and in a cloud job.
