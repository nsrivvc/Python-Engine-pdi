# Stage 1 — API -> JSON

The mock NatGasHub API. Stage 1 is nothing more than: serve the feeds over
HTTP, fetch one to a JSON file. The file is the stage's entire output — stage 2
(`../stage_2/json_to_raw.py`) takes it from there.

```
stage_1/
├── data/                JSON fixtures, served verbatim (edit a fixture to
│                        change what the API returns; no code changes)
├── src/mock_api/        the FastAPI app (app.py) + fixture loader (loader.py)
└── requirements.txt     fastapi + uvicorn — stage 1's only dependencies
```

## Run it locally

Run from THIS folder (the `src.mock_api.app` module path resolves against the
current working directory):

```bash
cd src/transformations/stage_1
pip install -r requirements.txt
python -m uvicorn src.mock_api.app:app --host 127.0.0.1 --port 8000
```

Then:

    http://127.0.0.1:8000/health              liveness probe
    http://127.0.0.1:8000/docs                interactive Swagger UI
    http://127.0.0.1:8000/api/firms           -> data/firm_transco_nationalgrid_10_new.json
    http://127.0.0.1:8000/api/interruptibles  -> data/interruptibles_test.json
    http://127.0.0.1:8000/api/ioc             -> data/ioc_test.json
    http://127.0.0.1:8000/api/awards          -> data/awards_test.json

Fetch a feed to a file (this IS stage 1's output):

```bash
curl -sf http://127.0.0.1:8000/api/awards -o data/_fetched_awards.json
```

The `_fetched_*.json` name matters: stage 2 routes a file to its raw table by
the words in its file name (awards -> bronze.gawd, firms -> bronze.gtran_firm,
interruptibles -> bronze.gtran_it, ioc -> bronze.gindex).

## In CI

Each `bronze_ingest_*.yml` workflow starts its own copy of this API inside the
runner, curls the endpoint into `data/_fetched_*.json`, hands that file to
stage 2 (`python src/transformations/stage_2/json_to_raw.py --file ... --create-tables`),
and the server dies with the job.

## Pointing at the real NatGasHub later

Drop the "Start mock" step from the workflows and curl the live endpoint (with
its API-key secret) instead. Nothing in stage 2 changes — it only ever sees a
JSON file.
