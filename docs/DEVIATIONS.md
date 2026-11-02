# Deviations & Notes

Anything that differed from the build prompt, why it happened, and what was done about it. All fixes to already-committed files were made in their own `fix(scope): …` commit.

## D1 — Warehouse host port 5433 → 5434

* **Prompt:** warehouse published on host port **5433**.
* **Reality:** a pre-existing local `postgres` service (pid 7176) already listens on 5433 on this machine.
* **Change:** `docker-compose.yml` publishes `5434:5432` instead of `5433:5432`; `Makefile` `up` echo and `README`/docs quote 5434.
* **Impact:** none on the design — only the host-side connection string (`localhost:5434`).

## D2 — Airflow UI host port 8080 → 8082

* **Prompt:** Airflow webserver on host port **8080**.
* **Reality:** another user container (`shopsphere_adminer`) already holds 8080, and 8081 is held by `mongo_express`.
* **Change:** `docker-compose.yml` publishes `8082:8080`. The `make up` banner and the dashboard's "trigger the DAG at …" info text were updated to match — committed separately as `fix(compose): use host port 8082 for airflow ui because 8080 is busy`.
* **Impact:** Airflow UI is at **http://localhost:8082** (inside the network nothing changed: containers still talk to 8080).

## D3 — No `make` and no `python3` on the host

* **Prompt:** use the `Makefile` targets and `python3 -m pytest`.
* **Reality:** this Windows host has **no `make`** (and installing it was out of scope) and no `python3` binary — only `python` 3.13, shell is PowerShell.
* **Change:**
  * Every verification was run as the **raw `docker compose` equivalent** of the target (the exact command table lives in `README.md`, section “No make? Use these commands”).
  * Makefile **syntax was validated** by running `make -n up` inside an `alpine` container with make installed (`MAKE_SYNTAX_OK`) — the recipe lines are real TABs.
  * All Python tests run **inside the Airflow container** (`docker compose run --rm --no-deps airflow-scheduler python -m pytest …`), so no host Python is needed at all.
* **Impact:** none on deliverables — the `Makefile` is complete and works on any host that has `make`.

## D4 — PowerShell mangling of `--conf '{"target_date": …}'`

* **Prompt:** `airflow dags trigger weather_pipeline --conf '{"target_date": "YYYY-MM-DD"}'`.
* **Reality:** PowerShell's native-argument parsing strips the embedded double quotes, so Airflow received `{target_date: …}` and failed with `JSONDecodeError: Expecting property name enclosed in double quotes`.
* **Change:** pass backslash-escaped JSON that survives PowerShell's quoting rules:
  ```powershell
  docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
  ```
  Python's `json.loads` accepts `\"` escapes, so the DAG receives exactly `{"target_date": "2026-10-30"}` (confirmed by the `conf` column in the trigger output: `{'target_date': '2026-10-30'}`).
* **Impact:** none on the pipeline — only the host-side command spelling. On a bash host the prompt's original single-quoted form works as written.

## D5 — `.gitignore` replaced (repo was not empty)

* **Prompt:** if the repo is not empty, take the given `.gitignore` content.
* **Reality:** the repository already had an `Initial commit` with its own `.gitignore`/`.gitattributes`.
* **Change:** the prompt's `.gitignore` (and `.gitattributes`) replaced the pre-existing ones in the first chore commit (`chore: add gitignore and gitattributes`). Safety verified afterwards:
  * `git check-ignore -v .env` → ignored; `.env`, `logs/*`, `data/raw/*`, `data/silver/*` (except `.gitkeep`), `__pycache__`, `*.parquet` are all untracked.
  * `git ls-files` shows no secrets, no data files.
* **Impact:** none — the previous `.gitignore` was a default that did not cover this project's artefacts.

## D6 — Open-Meteo returned HTTP 503 during verification (API incident)

* **What happened:** after ~30 minutes of intense verification traffic (including the `api_down` fault demo, which fires ~160 rapid failing requests), `api.open-meteo.com` **and** `archive-api.open-meteo.com` began answering `503 {"error":true,"reason":"The service is overloaded"}` for every request from this IP for roughly 45 minutes. Two runs failed because of it: the V6 recovery attempt (`manual__2026-11-02T15:22:07`) and the first V7 backfill attempt (failed on 2026-10-25).
* **Pipeline behaviour (exactly as designed):** HTTP-level `Retry(total=3, backoff_factor=1.0)` for 503 + Airflow's 3 retries (20/40/80 s) → after ~4 tries the run failed with a `ConnectionError`, an `ops.alerts` row was written (`try_number = 4`), and **nothing was loaded** (silver/gold counts unchanged).
* **Resolution:** no code change. A cooldown probe loop found the API recovered after ~45 min; the V6 rerun, the 3-day V7 backfill and the final clean run all succeeded afterwards.
* **Note per prompt rule 8:** the suggested fallback (`archive-api.open-meteo.com`) was tested and returned the **same 503**, i.e. it was an IP-level throttle on Open-Meteo's side, not a request-shape problem — so switching endpoints would not have helped and `extract.py` was left unchanged.

## D7 — One transient Airflow internal error during backfill (informational)

The backfill run printed one `DeadlockDetected` traceback from Airflow's own metadata-DB bookkeeping (`backfill_job_runner` updating `dag_run`) while two runs were transitioning simultaneously. Airflow logged it, the progress counter continued, and all 3 backfill runs finished `success` (24/24 tasks, `failed: 0`). No action taken — it is an Airflow-internal race, not a project defect.

## D8 — Host restart / Docker Desktop restart mid-verification (informational)

Docker Desktop (and the machine) restarted once during verification. All containers came back with `restart: unless-stopped`, volumes persisted, the in-flight `api_down` run correctly ended `failed`, and verification resumed from the same state. No data or history was lost.

---

**Not deviated from:** Airflow stays at **2.10.5** (no 3.x); the DAG contains no `from __future__ import annotations`; all warehouse access in container code uses `psycopg2`/`PostgresHook` with explicit SQL (no `read_sql`/`to_sql`); no TODOs/placeholders; every commit was pushed to `origin main`; no branches/PRs/force-pushes; `.env` and all generated data stay out of git.
