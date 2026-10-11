# Open-Meteo Weather Pipeline

A modular Python-file project for Databricks. It fetches hourly weather forecasts for five configured cities and writes Delta tables in Bronze, Silver, and Gold layers. GitHub Actions runs unit tests and deploys through Databricks Asset Bundles.

## Folder structure

```text
.github/workflows/deploy.yml
resources/weather_job.yml
src/weather_pipeline/
  common/       # configuration, API client, schema drift checks, email helper
  bronze/       # API ingestion and retry behavior
  silver/       # validation, quarantine, idempotent merge
  gold/         # daily aggregation and idempotent merge
  entrypoint.py # Python-file task entrypoint
tests/
sql/
databricks.yml
pyproject.toml
```

## Behavior and retry policy

- Each city request gets **3 total attempts**: the first request plus **2 retries**.
- A city that still fails is recorded in `weather_location_audit` and included in an email summary. Other successful cities continue through the pipeline.
- If **all selected city requests fail**, Bronze ingestion fails after sending an email summary. Silver and Gold are skipped.
- Invalid hourly records are written to `weather_rejected_hourly_records`. If at least some valid rows remain, Silver processes those rows and emails a city/date summary. If **no valid hourly rows remain**, Silver fails and Gold is skipped.
- Every Bronze response is checked against the expected Open-Meteo fields. Added, missing, or type-changed fields are appended to `weather_schema_audit` and logged; the raw payload remains available in Bronze for investigation.
- Each Bronze, Silver, and Gold task execution has one `weather_job_audit` row with run ID, layer, status, start/end time, parameters, failure reason, and input/output/rejected row counts. Counts represent attempted locations and ingested payloads in Bronze, Bronze payloads and valid/quarantined hourly records in Silver, and Silver hourly rows and daily aggregates in Gold. The row is updated from `RUNNING` to `SUCCESS` or `FAILED`; a rerun gets a separate row even when it reuses the same run ID. A hard process termination can leave a `RUNNING` row for investigation.
- Silver merges on `(location_id, forecast_time)`. Gold merges on `(location_id, forecast_date)`. Re-running a city/date range updates existing logical records instead of duplicating them.
- Bronze is append-only by design and stores each successful pull.

## Run only selected cities or dates

The Databricks Job has overridable parameters:

- `weather_cities`: `all` by default, or a comma-separated list of city IDs such as `bengaluru,delhi`.
- `weather_start_date`: optional ISO date such as `2026-10-08`.
- `weather_end_date`: optional ISO date such as `2026-10-10`. Set both dates together.
- `weather_api_url`: target-specific Open-Meteo-compatible endpoint. Defaults to `https://api.open-meteo.com/v1/forecast`.
- `weather_forecast_days`: target-specific forecast length when no date range is specified. Defaults are 3 days in dev, 5 in test, and 7 in prod; valid values are 3–7.
- `WEATHER_CATALOG`, `WEATHER_SCHEMA`, and `WEATHER_AUDIT_SCHEMA`: target Unity Catalog catalog/data schema/audit schema (defaults `workspace` / `weather_api_dev` / `audit`).

The endpoint and forecast length are Databricks Asset Bundle variables (`api_url` and `forecast_days`) and can be overridden per target in `databricks.yml` or with `BUNDLE_VAR_api_url` / `BUNDLE_VAR_forecast_days` during deployment. The orchestrator passes both settings to each child job, and the entrypoint sets `OPEN_METEO_API_URL` and `WEATHER_FORECAST_DAYS` before importing pipeline configuration.

Use **Run now with different parameters** on `weather_pipeline_orchestrator` to run the full chain for selected cities/dates. To recover a single layer, run `bronze_pipeline`, `silver_pipeline`, or `gold_pipeline` directly with matching parameters. Keep the same `run_id` when running Silver against a particular Bronze ingestion. The orchestrator run ID is passed to all child jobs.

**Date-range limitation:** the Open-Meteo forecast endpoint only serves dates in its supported forecast/past window. For older historical dates, switch to the appropriate Open-Meteo historical API before running a backfill.

## Email configuration for partial failures

Databricks job-level `email_notifications.on_failure` sends a failure notification when a task fails. For partial city/date failures where the job should continue, configure SMTP so the Python code can send a warning email. Set these as secure runtime environment variables (use your workspace's approved secret mechanism; do not commit credentials):

- `WEATHER_SMTP_HOST`
- `WEATHER_SMTP_PORT` (default `587`)
- `WEATHER_SMTP_FROM`
- `WEATHER_ALERT_EMAIL`
- `WEATHER_SMTP_USERNAME` (optional, if authentication is required)
- `WEATHER_SMTP_PASSWORD` (optional, if authentication is required)
- `WEATHER_SMTP_STARTTLS` (default `true`)

If SMTP isn't configured, the pipeline logs the detailed warning, but cannot send a partial-failure email. Configure the email in the job YAML too for task failures.

## Testing locally

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -e ".[dev]"
pytest -v
```

Unit tests cover pure validation, API response edge cases, retry behavior, and schema drift detection. Integration testing of Spark/Delta operations must be done in Databricks.

## Dashboard queries

`sql/dashboard_queries.sql` contains dashboard tile queries for city weather comparisons, precipitation, latest API status, ingestion/rejection counts, Silver freshness, API schema changes, and job status by layer. The queries target `workspace.weather_api_dev`; change the schema for the test or production workspace as needed. Run each query in Databricks SQL and add it as a visualization/tile to a dashboard. This repository contains the query set, but the dashboard itself must be created in the workspace.

## GitHub Actions secrets

For GitHub deployment, add workspace-specific secrets to GitHub Environments. See [Multi-workspace deployment](#multi-workspace-deployment-dev--test--prod). For a local deployment, set these environment variables in your shell:

- `DATABRICKS_HOST`: workspace URL
- `DATABRICKS_TOKEN`: supported token for your workspace

Never commit credentials.

## Deploy manually from PowerShell

```powershell
$env:DATABRICKS_HOST = "https://<your-workspace-host>"
$env:DATABRICKS_TOKEN = "<your-token>"
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run weather_pipeline_orchestrator -t dev
```

## Pipeline dependency graph

```text
weather_pipeline_orchestrator
    -> bronze_pipeline (independent Databricks Job)
    -> silver_pipeline (only after Bronze succeeds)
    -> gold_pipeline (only after Silver succeeds)
```

If every selected city fails, Bronze fails and Silver/Gold do not run. If some cities fail but others succeed, Bronze sends a warning and succeeds so Silver and Gold can process successful cities.

## Tables

- `workspace.<target_schema>.weather_api_raw` (Bronze)
- `workspace.<target_schema>.weather_hourly_transformed` (Silver)
- `workspace.<target_schema>.weather_daily_aggregation` (Gold)
- `workspace.audit.weather_rejected_hourly_records`
- `workspace.audit.weather_ingestion_audit`
- `workspace.audit.weather_api_schema_changes`
- `workspace.audit.weather_job_audit`

The target schemas are `weather_api_dev`, `weather_api_test`, and `weather_api_prod`.

## Important

This is a starter implementation and has not been deployed or integration-tested in your Databricks workspace. Validate the bundle and verify that your workspace supports the configured Python-file task and serverless environment specification before running.


## Multi-workspace deployment (dev → test → prod)

Each bundle target connects to its own Databricks workspace. The same account can contain all three workspaces; set each target's host and token in its corresponding GitHub Environment.

| Git branch | Bundle target | GitHub Environment | Databricks workspace | Data schema |
|---|---|---|---|---|
| `dev` | `dev` | `dev` | Development workspace | `workspace.weather_api_dev` |
| `test` | `test` | `test` | Test/staging workspace | `workspace.weather_api_test` |
| `main` | `prod` | `prod` | Production workspace | `workspace.weather_api_prod` |

### Configure GitHub Environments and secrets

In **GitHub → Settings → Environments**, create environments named exactly `dev`, `test`, and `prod`. In each environment, add these environment secrets and variable:

- `DATABRICKS_HOST`: that workspace's URL, such as `https://<workspace-id>.cloud.databricks.com`
- `DATABRICKS_TOKEN`: a credential scoped to that workspace
- Variable `WEATHER_ALERT_EMAIL`: the notification recipient for that environment

Do not add a single repository-wide `DATABRICKS_HOST` or `DATABRICKS_TOKEN`; the deploy job selects the GitHub Environment that matches the target, and GitHub supplies that environment's secrets. Never commit credentials. Add required reviewers to the `prod` GitHub Environment if production deployment needs approval.

Pull requests to `dev`, `test`, and `main` run tests but do not deploy. A push to one of those branches runs tests, selects its matching target, validates and deploys to that workspace, then starts the orchestrator. A manual workflow run lets you choose `dev`, `test`, or `prod`.

### First-time setup

1. Create or identify the three Databricks workspaces under your account.
2. In each workspace, confirm its URL and ensure the deployment identity can create jobs and write to the target catalog/schema.
3. Configure the three GitHub Environments and their secrets as described above.
4. Push to `dev` first and confirm the workflow validates, deploys, and completes a run in the development workspace.
5. Promote changes through `test`, then `main`/`prod`.

For local manual deployment, set `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `BUNDLE_VAR_notification_email` to the values for the workspace you intend to target, then run the matching `databricks bundle validate/deploy/run -t <target>` commands. Verify that each workspace supports the configured Python-file task and serverless environment specification before relying on automated runs.
