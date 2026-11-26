# Course Project Assignment: Enterprise Lakehouse & Medallion Pipeline with Apache Spark

## 1. Scenario & Objective

You are joining the Data Engineering team of a mid-sized consulting and technology company (**CorpTech Consulting**). Each day, operational source systems across **HR**, **Finance**, **Business/CRM**, and **People Analytics (eNPS)** export raw batch files into a shared **Landing Zone** (`./data/landing`).

Your objective is to design, implement, orchestrate, and test an end-to-end **Apache Spark Lakehouse** following the **Medallion Architecture (`Bronze → Silver → Gold`)**, backed by automated **Data Quality (DQ) checks**, **workflow orchestration**, and a **lightweight reporting layer** built on top of the Gold layer.

By the end of the project, your platform must:
1. **Acquire & ingest** pre-generated daily partitioned and full-load raw files reliably into a **Bronze** layer.
2. **Clean, validate, conform, and enrich** the data in a **Silver** layer (including **SCD Type 2**, quarantine tables, currency normalization, and Spark shuffle-skew mitigation).
3. **Model business-ready Gold data marts** (financial profitability, HR & eNPS analytics, CRM engagement, and an ML-ready churn feature table).
4. **Orchestrate** daily incremental runs and historical backfills with automated **Data Quality gates**.
5. **Visualize** key business KPIs and pipeline DQ health in a **simple Reporting Layer**.

---

## 2. Obtaining the Dataset

A pre-generated **200-day corporate development dataset** (`2026-01-01` to `2026-07-19`, Days 1–200 across 10 datasets) is provided for this course via **Cloudflare R2**. An additional **70-day holdout slice** (`2026-07-20` to `2026-09-27`, Days 201–270) is reserved for final grading/defense to evaluate incremental ingestion, backfill idempotency on unseen daily drops, and out-of-time ML churn generalization.

You must download and verify the 200-day dataset into your local landing zone (`./data/landing`) before running your pipelines.

### Option A — Manifest-Driven Download from Cloudflare R2 (Recommended)
1. Copy `.env.example` to `.env` and fill in the Cloudflare R2 credentials provided by your instructor:
   ```dotenv
   R2_ACCOUNT_ID=<YOUR_R2_ACCOUNT_ID>
   R2_BUCKET_NAME=<YOUR_R2_BUCKET_NAME>
   R2_PREFIX=course-2026
   R2_STUDENT_ACCESS_KEY_ID=<YOUR_R2_STUDENT_ACCESS_KEY_ID>
   R2_STUDENT_SECRET_ACCESS_KEY=<YOUR_R2_STUDENT_SECRET_ACCESS_KEY>
   ```
2. Run the automated downloader script, which reads `MANIFEST.sha256` under `R2_PREFIX`, downloads every archive listed in it, verifies SHA-256 checksums before unpacking, extracts the payload into `./data/landing`, and checks all 10 dataset folders:

   ```bash
   # macOS / Linux / Git Bash
   ./scripts/download_data.sh
   ```
   ```powershell
   # Windows (PowerShell)
   .\scripts\download_data.ps1
   ```

### Option B — Direct HTTP Archive Override (Optional Mirror)
```bash
./scripts/download_data.sh --url "<DATASET_DOWNLOAD_URL>"
```

### Verify Your Download
You can re-verify checksums and all 10 required folders (`clients`, `projects`, `survey_questions`, `employees`, `timesheets`, `fx_rates`, `invoices`, `expenses`, `crm_events`, `employee_surveys`) at any time without re-downloading:

```bash
./scripts/download_data.sh --verify-only
```

- **Do not commit the downloaded `./data/` folder or `.env` credentials to Git** — both are listed in `.gitignore`.
- For rapid local iteration and unit tests, iterate on a shorter 14-day slice (`2026-01-01` to `2026-01-14`), then validate your full pipeline and reporting layer on the complete **200-day** student dataset. Design all ingestion and transformation jobs incrementally so your pipeline can seamlessly process the **70-day holdout** (`2026-07-20` to `2026-09-27`) during evaluation.

---

## 3. Data Description (Landing Zone)

The Landing Zone contains full-load reference datasets alongside daily batch drops organized using Hive-style directory partitioning (`date=YYYY-MM-DD/part-000.<ext>`):

```text
data/landing/
├── clients/clients.csv
├── projects/projects.csv
├── survey_questions/survey_questions.json
├── employees/date=YYYY-MM-DD/part-000.csv
├── timesheets/date=YYYY-MM-DD/part-000.parquet
├── fx_rates/date=YYYY-MM-DD/part-000.json
├── invoices/date=YYYY-MM-DD/part-000.jsonl
├── expenses/date=YYYY-MM-DD/part-000.csv
├── crm_events/date=YYYY-MM-DD/part-000.jsonl
└── employee_surveys/date=YYYY-MM-DD/part-000.jsonl
```

### 3.1 Business / CRM Domain

| Dataset | Format & Load Type | Columns & Semantics |
| :--- | :--- | :--- |
| **`clients`** | CSV (Full Load) | `client_id` (`string`, PK; includes `VIP_CORP_001` and `CLI_0002`..), `client_name` (`string`), `tier` (`Standard`, `Silver`, `Gold`, `Enterprise`). |
| **`projects`** | CSV (Full Load) | `project_id` (`string`, PK; `PRJ_0001`..), `client_id` (`string`, FK → `clients`), `budget_eur` (`double`), `status` (`Planned`, `Active`, `Completed`, `OnHold`). |
| **`crm_events`** | JSONL (Daily Batch) | `event_id` (`string`), `client_id` (`string`, FK → `clients`), `event_type` (`page_view`, `support_ticket`, `contract_renewal`, `api_call`, `meeting_logged`), `timestamp` (`ISO-8601 UTC`), `payload` (`object` — **nested JSON whose schema varies by `event_type`**). |

> **Operational Note (Skew):** One enterprise account (`VIP_CORP_001`) accounts for roughly **45% of all CRM events**, introducing severe key skew during distributed joins and wide aggregations.

### 3.2 Human Resources Domain

| Dataset | Format & Load Type | Columns & Semantics |
| :--- | :--- | :--- |
| **`employees`** | CSV (Daily Full Snapshot) | `emp_id` (`string`), `full_name` (`string`), `department` (`Engineering`, `Data`, `Consulting`, `Sales`, `Finance`, `Operations`), `role` (`string`), `seniority_level` (`Junior`, `Mid`, `Senior`, `Lead`, `Principal`), `hourly_rate_eur` (`double`), `hire_date` (`date`), `last_promotion_date` (`date`), `work_mode` (`Remote`, `Hybrid`, `Onsite`), `distance_km` (`double`), `manager_id` (`string`, nullable FK → `emp_id`), `is_active` (`boolean`), `termination_date` (`date`, nullable), `churn_type` (`voluntary` \| `involuntary`, nullable), `snapshot_date` (`date`). |
| **`timesheets`** | Parquet (Daily Batch) | `ts_id` (`string`), `emp_id` (`string`, FK → `employees`), `project_id` (`string`, nullable FK → `projects`), `work_date` (`date`), `hours` (`double`). |

> **Operational Notes (HR):**
> - Roughly 10% of employees change `department`, `role`, and/or `hourly_rate_eur` across the 200-day development period (and into the 70-day holdout), requiring SCD Type 2 tracking for accurate historical labor cost attribution.
> - Employee departures occur throughout the 200-day development period and the 70-day holdout (`is_active` transitions to `false` and `termination_date` is populated from the departure date onward).

### 3.3 Employee eNPS Surveys Domain

| Dataset | Format & Load Type | Columns & Semantics |
| :--- | :--- | :--- |
| **`survey_questions`** | JSON (Full Load Catalog) | Array of 10 question definitions (`question_id`, `dimension`, `scale_min`, `scale_max`, `prompt_text`): `Q01` (`enps`, `0–10`) plus 9 Likert (`1–5`) drivers: `Q02` (`work_life_balance`), `Q03` (`compensation_fairness`), `Q04` (`manager_support`), `Q05` (`career_growth`), `Q06` (`role_clarity`), `Q07` (`team_collaboration`), `Q08` (`tools_and_processes`), `Q09` (`leadership_vision`), `Q10` (`recognition`). |
| **`employee_surveys`** | JSONL (Daily Pulse Batches) | `survey_id` (`string`), `emp_id` (`string`), `submitted_at` (`ISO-8601 UTC`), `enps_score` (`int`, corresponds to `Q01`), `overall_comment` (`string`), `answers` (`array<struct<question_id, dimension, numeric_score, written_answer>>` — 10 entries per submission). |

### 3.4 Finance Domain

| Dataset | Format & Load Type | Columns & Semantics |
| :--- | :--- | :--- |
| **`fx_rates`** | JSON (Daily, **Weekdays Only**) | JSON array of 6 currency pairs (`EUR`, `USD`, `CZK`): `rate_date` (`string`), `currency_from`, `currency_to`, `rate` (`double`). **No files are delivered for Saturdays and Sundays.** |
| **`invoices`** | JSONL (Daily Batch) | `inv_id` (`string`), `client_id` (`string`), `project_id` (`string`), `amount` (`double`), `currency` (`EUR` \| `USD` \| `CZK`), `issued_at` (`string`), `paid_at` (`string`, nullable — open/unpaid invoices are `NULL`). |
| **`expenses`** | CSV (Daily Batch) | `exp_id` (`string`), `department` (`string`), `category` (`Cloud Infrastructure`, `Software Licenses`, `Travel & Lodging`, `Training`, `Hardware`, `Office & Events`, `Subcontractors`), `amount` (`double`), `currency` (`EUR` \| `USD` \| `CZK`), `expense_date` (`date`). |

### 3.5 Known Upstream Data Quality Issues

As in real enterprise ingestion environments, the raw Landing Zone files contain anomalies that your pipelines must detect, log, and handle properly:
- **Exact & near duplicates** in `timesheets`, `expenses`, and `employee_surveys`.
- **Inconsistent date formats** in `invoices` (mix of ISO `YYYY-MM-DD` and European `DD.MM.YYYY` strings).
- **Missing & orphan foreign keys**: `NULL` `project_id` in `timesheets`, and invoices referencing unknown `project_id` values (`PRJ_ORPHAN_*`).
- **Domain anomalies**: `timesheets` with `hours <= 0` or `hours > 24`; post-termination timesheet entries (`work_date >= termination_date`); `employee_surveys` with out-of-range scores (`-1`, `11`) or empty/`NULL` `written_answer`.
- **Valid business edge cases (do NOT discard as errors)**: Negative `amount` values in `invoices` represent **credit notes**; unpaid invoices legitimately have `paid_at = NULL`; missing weekend `fx_rates` must be forward-filled (LOCF) from the most recent business day.

---

## 4. Technical Requirements

### 4.1 Core Technology Stack
- **Processing Engine:** Apache Spark (`PySpark 3.5+`).
- **Storage Format:** **Delta Lake** (recommended) or Apache Iceberg / partitioned Parquet for `Bronze`, `Silver`, and `Gold` zones.
- **Orchestration:** **Apache Airflow** (recommended; Dagster or Prefect accepted if clearly justified).
- **Data Quality:** Reusable PySpark DQ validation layer, **Great Expectations**, **Soda Core**, or **PyDeequ**, persisting structured audit logs.
- **Reporting Layer:** **Streamlit**, **Apache Superset**, **Metabase**, or **DuckDB + BI/Notebook Dashboard** reading strictly from Gold & DQ audit tables.

---

### 4.2 Medallion Layer Specifications

#### A. Bronze Layer (Raw Ingestion)
- Ingest all 10 Landing Zone datasets incrementally (by `batch_date` for daily tables; overwrite/versioned snapshot for full-load tables).
- Preserve raw payloads with **zero lossy transformations** (keep raw date strings in `invoices`, keep raw nested JSON payloads in `crm_events` and `employee_surveys`).
- Append standardized technical metadata columns to every Bronze table:
  - `_batch_date` (`date`)
  - `_ingested_at` (`timestamp`)
  - `_source_file` (`string`)
- Must be **idempotent**: re-running a given `batch_date` must not duplicate records in Bronze.

#### B. Silver Layer (Cleansed, Conformed & Quarantined)
Implement a **Quarantine pattern**: invalid records must be routed to `silver_quarantine.<table_name>` with a `dq_reasons` (`array<string>`) column and `_batch_date`, rather than silently dropped.

Required transformations:
1. **`silver_employees_scd2` (SCD Type 2):**
   - Build Slowly Changing Dimension Type 2 history (`valid_from`, `valid_to`, `is_current`) tracking changes in `department`, `role`, `hourly_rate_eur`, `is_active`, `termination_date`, and `churn_type`.
2. **`silver_timesheets`:**
   - Deduplicate exact and near-duplicates by `ts_id` (document your deterministic tie-breaker rule).
   - Quarantine rows where `project_id IS NULL`, `hours <= 0`, `hours > 24`, or `work_date >= employee.termination_date`.
3. **`silver_fx_rates_daily`:**
   - Generate a complete calendar spine (`start_date`..`end_date`) for all currency pairs and forward-fill missing weekend/holiday rates using Spark window functions (`last(..., ignorenulls=True)`).
4. **`silver_invoices`:**
   - Parse both `YYYY-MM-DD` and `DD.MM.YYYY` into canonical `DATE` columns (`issued_date`, `paid_date`).
   - Quarantine records failing referential integrity against `silver_projects` (`left_anti` check).
   - Retain negative amounts as valid **credit notes** (`is_credit_note: boolean`, `net_amount_eur`).
   - Join with `silver_fx_rates_daily` to compute `amount_eur` (EUR rows use rate `1.0`).
5. **`silver_expenses`:**
   - Deduplicate exact and near-duplicates by `exp_id` and compute `amount_eur` via `silver_fx_rates_daily`.
6. **`silver_crm_events`:**
   - Parse `timestamp` to UTC `TIMESTAMP` and extract key structured attributes from the polymorphic `payload` JSON (retaining `payload_raw` or typed struct fields per `event_type`).
   - Join/enrich with `silver_clients` using an explicit **skew-handling strategy** (e.g., Broadcast Join, Spark AQE skew join optimization, or key salting) and include **Spark UI evidence** (screenshots or metrics) comparing skewed vs. optimized execution.
7. **`silver_survey_responses` & `silver_survey_answers`:**
   - Deduplicate by `survey_id`.
   - `explode()` the 10-element `answers` array and join with `survey_questions` metadata.
   - Quarantine answers violating `[scale_min, scale_max]` bounds; handle null/blank `written_answer` cleanly (flag `has_written_comment`).
   - Derive `enps_category` (`Promoter`: `9–10`, `Passive`: `7–8`, `Detractor`: `0–6`).

#### C. Gold Layer (Analytics Marts & ML Feature Table)
Create at least the following **5 Gold tables**:

1. **`gold_project_profitability`:**
   - Grain: `(project_id, year_month)` (and overall project summary view).
   - Metrics: `invoiced_revenue_eur`, `credit_notes_eur`, `net_revenue_eur`, `billable_hours`, `labor_cost_eur` (**must join `silver_timesheets` with `silver_employees_scd2` using the employee's point-in-time `hourly_rate_eur` valid on `work_date`**), `gross_margin_eur`, `gross_margin_pct`, `budget_utilization_pct`.
2. **`gold_department_finance`:**
   - Grain: `(department, category, year_month)`.
   - Metrics: `total_expense_eur`, `expense_count`, departmental headcount & billable utilization.
3. **`gold_client_engagement`:**
   - Grain: `(client_id, client_tier, event_type, event_date)`.
   - Metrics: `event_count`, `avg_api_latency_ms`, `critical_ticket_count`, `pipeline_arr_delta_eur`.
4. **`gold_hr_enps_analytics`:**
   - Grain: `(department, seniority_level, work_mode, survey_wave)`.
   - Metrics: `respondent_count`, `enps_score` (`% Promoters − % Detractors`), average score per driver (`Q02`–`Q10`), `voluntary_attrition_rate`, `involuntary_attrition_rate`.
5. **`gold_employee_churn_features` (Point-in-Time ML Feature Table):**
   - Grain: `emp_id` (leak-free snapshot cut at day 17 / Wave 1 so features can be trained/validated on the 200-day student dataset and evaluated out-of-time on the 70-day holdout).
   - Features: `compa_ratio` (employee hourly rate ÷ `(department, seniority_level)` peer median), `tenure_years`, `months_since_promotion`, `work_mode`, `distance_km`, `manager_id`, early average daily hours & weekend work counts from `timesheets`, Wave 1 `enps_score` & `Q02–Q10` numeric scores, and future target `churned_within_quarter` (`0/1`, target label for subsequent churn in the prediction horizon).

---

### 4.3 Data Quality (DQ) Checks Framework

Implement automated DQ gates integrated into the orchestration flow and persist all check executions into a **`dq_audit_results`** table (`run_id`, `batch_date`, `layer`, `table_name`, `check_name`, `severity`, `status`, `failed_count`, `total_count`, `checked_at`):

- **Hard Checks (`severity = ERROR` — must fail the task/pipeline):**
  - Expected schema & non-null primary keys in Silver/Gold.
  - Non-empty daily partitions for mandatory tables (properly accounting for the absence of weekend `fx_rates`).
  - Uniqueness of `(emp_id, valid_from)` in `silver_employees_scd2` with no overlapping validity intervals.
  - Zero `NULL` EUR amounts after FX conversion (`silver_invoices`, `silver_expenses`).
- **Soft Checks (`severity = WARN` — log metric, quarantine bad rows, continue pipeline):**
  - Quarantine ratio thresholds (e.g., warn if > 8% of rows are quarantined in a single batch).
  - Orphan invoice rate, duplicate rate, zombie timesheet count, missing written survey comments.

---

### 4.4 Orchestration Requirements

Build an orchestrated workflow (e.g., **Apache Airflow DAG**) with:
- **Parameterized daily execution** (`batch_date`, e.g., Airflow `{{ ds }}`) supporting both scheduled daily runs and multi-day **backfills**.
- **Clear task dependency graph:**
  ```text
  sense_landing_files
      └──▶ bronze_ingest (parallel per domain)
               └──▶ dq_bronze_gate
                        └──▶ silver_transform (fx & dimensions → facts & surveys)
                                 └──▶ dq_silver_gate
                                          └──▶ gold_marts (parallel)
                                                   └──▶ dq_gold_gate
                                                            └──▶ refresh_reporting_layer
  ```
- **Idempotency & Recovery:** Running the same `batch_date` twice produces identical Bronze/Silver/Gold/DQ tables without duplicate rows.
- **Retries & Logging:** Configure task retries, execution timeouts, and failure callback logging.

---

### 4.5 Simple Reporting Layer

Build a clean, lightweight reporting application on top of **Gold** and **`dq_audit_results`** (e.g., a **Streamlit** multi-tab app using DuckDB/Spark SQL, or Superset/Metabase dashboard). It must include **4 views**:

1. **Finance & Project Profitability:** Net revenue vs. credit notes (EUR), labor cost, top/bottom margin projects, budget burn, and departmental expense breakdown.
2. **HR, Attrition & eNPS Pulse:** Company & departmental eNPS score, 10-question driver breakdown (`Q02`–`Q10`), attrition breakdown by department / `work_mode` / `compa_ratio` bucket, and filterable written employee comments.
3. **CRM Client Engagement:** Event volume by client tier and `event_type`, `VIP_CORP_001` traffic share, API latency, and critical support ticket trends.
4. **Pipeline & Data Quality Health:** Daily rows ingested vs. quarantined, breakdown of DQ failures by rule & table, and pass/warn/fail run history from `dq_audit_results`.

---

## 5. Deliverables & Repository Structure

Submit a Git repository containing:

```text
project-root/
├── docker-compose.yml          # Reproducible local stack (Spark, Airflow, Reporting UI)
├── scripts/
│   ├── download_data.py        # Manifest-driven Cloudflare R2 downloader & verifier
│   ├── download_data.sh        # Automated Linux/macOS wrapper
│   └── download_data.ps1       # Automated Windows PowerShell wrapper
├── dags/                       # Orchestration DAG definitions
├── spark_jobs/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── dq/                         # Reusable DQ checks & audit logger
├── reporting/                  # Streamlit / BI dashboard code & SQL views
├── tests/                      # Unit & integration tests (PySpark + DQ rules)
├── docs/
│   ├── architecture.md         # Architecture diagram, SCD2 design, Spark UI skew analysis
│   └── runbook.md              # How to download data, run, backfill, and troubleshoot
└── README.md                   # Quickstart instructions
```

---

## 6. Evaluation Rubric (100 Points + 10 Bonus)

| Category | Weight | Key Criteria |
| :--- | :---: | :--- |
| **PySpark Medallion Pipelines (Bronze / Silver / Gold)** | **35 pts** | Correct schemas, true SCD Type 2 & point-in-time cost joins, weekend FX forward-fill, credit note handling, skew optimization with Spark UI proof. |
| **Data Quality Framework & Quarantine** | **20 pts** | Comprehensive hard/soft rules, zero silent data loss, quarantine tables with failure reasons, persisted `dq_audit_results`. |
| **Orchestration & Idempotency** | **20 pts** | Clean Airflow/orchestrator DAG, parameterized `batch_date` runs, verified backfill & re-run idempotency (including readiness for the 70-day evaluation holdout), retry/failure handling. |
| **Reporting Layer** | **15 pts** | Reads strictly from Gold/DQ tables, all 4 required business & DQ views functional, clear KPI definitions and filters. |
| **Code Quality, Tests & Documentation** | **10 pts** | Modular code, PySpark unit tests, automated download script, reproducible setup (`docker-compose` / `uv`), clear architecture notes. |
| **Bonus — Predictive Churn / NLP Analytics** | **+10 pts** | Train a Spark MLlib or scikit-learn churn model on `gold_employee_churn_features` (report ROC-AUC & top feature importances) **or** run sentiment/topic extraction on `written_answer` texts and surface insights in the HR dashboard. |
