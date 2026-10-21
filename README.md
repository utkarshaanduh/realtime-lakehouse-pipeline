# Real-Time Lakehouse Pipeline (CDC-based)

End-to-end data engineering project: CDC capture from Postgres -> Kafka -> Delta Lake (bronze) -> dbt transformations (silver/gold in BigQuery) -> BI dashboard, with data quality checks, CI/CD, and infra-as-code.

## Architecture
Neon Postgres --(CDC via Airbyte)--> Upstash Kafka --(Databricks Spark Streaming)--> Delta Lake on Cloudflare R2 (bronze) --(dbt Cloud)--> BigQuery (silver/gold) --> Looker Studio dashboard

Data quality: Great Expectations
Orchestration/CI-CD: GitHub Actions
Infra-as-code: Terraform

## Structure
- `infra/` - Terraform configs
- `ingestion/` - synthetic event generator (Faker)
- `dbt_project/` - dbt models (staging, marts)
- `spark_jobs/` - Databricks notebooks/scripts for bronze ingestion
- `dq/` - Great Expectations suites
- `.github/workflows/` - CI/CD and scheduled pipeline runs

## Status
Project scaffolded. Build phases tracked in project plan.
