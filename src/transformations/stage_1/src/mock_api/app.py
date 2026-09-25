"""
app.py
======
FastAPI application that impersonates NatGasHub. This is STAGE 1 of the
pipeline: the "external API" every ingest fetches from until the real
NatGasHub endpoint (plus its API-key secret) replaces it.

HOW TO RUN IT LOCALLY
---------------------
Run from THIS STAGE's root -- src/transformations/stage_1 -- because the
module path `src.mock_api.app` resolves against the current working
directory:

    cd src/transformations/stage_1
    python -m uvicorn src.mock_api.app:app --host 127.0.0.1 --port 8000

Add uvicorn's --reload flag to auto-restart on code edits (unrelated to the
pipeline's own --reload). Then:

    http://127.0.0.1:8000/health              liveness probe
    http://127.0.0.1:8000/docs                interactive Swagger UI
    http://127.0.0.1:8000/api/firms           -> data/firm_transco_nationalgrid_10.json
    http://127.0.0.1:8000/api/interruptibles  -> data/interruptibles_test.json
    http://127.0.0.1:8000/api/ioc             -> data/ioc_test.json
    http://127.0.0.1:8000/api/awards          -> data/awards_test.json

Each endpoint returns the matching fixture in this stage's data/ folder
VERBATIM (see loader.py) -- to change what the API serves, edit the fixture
file; no code changes needed. This service never touches Neon/Postgres.

IN CI, NOTHING TO DO MANUALLY: every bronze_ingest_*.yml workflow starts its
own copy on 127.0.0.1:8000 inside the runner ("Start mock NatGasHub API"
step), curls the endpoint into data/_fetched_*.json, feeds that file to
stage 2 (src/transformations/stage_2/json_to_raw.py), and the server dies
with the job.

CORS origins for browser-based consumers (e.g. the orchestration interface's
dev frontend) default to common local dev ports below and can be overridden
with a comma-separated MOCK_API_CORS_ORIGINS env var.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .loader import load_fixture

_DEFAULT_CORS_ORIGINS = (
    "http://localhost:3000,http://localhost:5173,http://localhost:8080"
)

app = FastAPI(
    title="Mock NatGasHub API",
    description="Serves the JSON fixtures in /data as if it were NatGasHub.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv("MOCK_API_CORS_ORIGINS", _DEFAULT_CORS_ORIGINS).split(",")
        if origin.strip()
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": "mock-natgashub-api"}


@app.get("/api/firms")
def get_firms():
    return load_fixture("firm_transco_nationalgrid_10.json")


@app.get("/api/interruptibles")
def get_interruptibles():
    return load_fixture("interruptibles_test.json")


@app.get("/api/ioc")
def get_ioc():
    return load_fixture("ioc_test.json")


@app.get("/api/awards")
def get_awards():
    return load_fixture("awards_test.json")
