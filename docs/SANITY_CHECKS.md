# 14-Day Data Sanity Checks & KPI Formula Glossary

Use this reference to verify your Bronze ingestion and Silver data-quality logic on the initial **14-day slice (`2026-01-01` through `2026-01-14`)** before scaling up to the full **200-day** backfill (`2026-01-01` – `2026-07-19`) and final evaluation on the **70-day holdout** (`2026-07-20` – `2026-09-27`).

---

## 1. Expected Raw & Silver Counts (`2026-01-01` – `2026-01-14`)

### A. Landing Zone / Bronze Raw Row Counts

| Dataset | Load Type | Partitions / Files (Days 1–14) | Expected Raw Rows |
| :--- | :--- | ---: | ---: |
| `clients` | Full Load CSV | 1 file | **300** |
| `projects` | Full Load CSV | 1 file | **800** |
| `survey_questions` | Full Load JSON | 1 JSON array | **10** |
| `employees` | Daily Snapshot CSV | 14 daily partitions | **16,800** (1,200 / day) |
| `timesheets` | Daily Parquet | 14 daily partitions | **34,675** |
| `fx_rates` | Weekday-Only JSON | **10** partitions (4 weekend days omitted) | **60** (6 pairs / weekday) |
| `invoices` | Daily JSONL | 14 daily partitions | **4,445** |
| `expenses` | Daily CSV | 14 daily partitions | **6,894** |
| `crm_events` | Daily JSONL | 14 daily partitions | **42,507** |
| `employee_surveys` | Daily Pulse JSONL | Pulse Wave 1 (Days 9–16; Days 9–14 here) | **760** |

### B. Silver Data Quality & Skew Expectations (Days 1–14)

- **`timesheets`:**
  - Total raw rows: **34,675** (`33,051` distinct `ts_id` values; **1,624** extra duplicate/near-duplicate rows)
  - Raw anomalies present before deduplication: **1,742** rows with `project_id IS NULL`, **876** rows with `hours <= 0`, **908** rows with `hours > 24`
  - Expected valid rows after deduplicating on `ts_id` (keeping first) and quarantining invalid rows (`NULL project_id`, `hours <= 0`, `hours > 24`): **~29,771** *(exact count varies slightly depending on your deterministic tie-breaker between exact and near-duplicates)*
- **`fx_rates`:**
  - Days `2026-01-03`, `2026-01-04`, `2026-01-10`, and `2026-01-11` (Saturdays & Sundays) have no raw files.
  - After generating a full 14-day calendar spine (`14 × 6 pairs`), `silver_fx_rates_daily` must contain **84 rows** with **0 NULL rates**.
- **`invoices`:**
  - Total raw rows: **4,445**
  - European date strings (`DD.MM.YYYY`) in `issued_at`: **211** rows (must parse cleanly alongside ISO `YYYY-MM-DD`)
  - Orphan `project_id` references (`PRJ_ORPHAN_*`) to quarantine: **43** rows (`~0.97%`)
  - Valid negative **credit notes** (`amount < 0`, retained in Silver): **232** rows
  - Open / unpaid invoices (`paid_at IS NULL`, valid): **1,352** rows
- **`expenses`:**
  - Total raw rows: **6,894** → **6,552** distinct `exp_id` values (**342** extra duplicate/near-duplicate rows)
- **`crm_events`:**
  - Total raw rows: **42,507**
  - Skew check: `VIP_CORP_001` accounts for **19,151** events (**45.05%**)
- **`employee_surveys`:**
  - Raw submissions (Days 1–14): **760** → **746** distinct `survey_id` values (14 duplicate submissions)
  - Exploded answer-level DQ issues: **18** out-of-range numeric scores (`-1` or `11`) and **40** blank/`NULL` `written_answer` entries

---

## 2. KPI Formula & Business Rule Glossary

Use these canonical definitions across Gold marts and the Reporting Layer so metrics are consistent across teams:

1. **eNPS Score (`-100` to `+100`):**
   $$\text{eNPS} = \left(\frac{\text{Promoters (Q01 } \in \{9,10\})}{\text{Total Respondents}} - \frac{\text{Detractors (Q01 } \in \{0..6\})}{\text{Total Respondents}}\right) \times 100$$
   *(Passives `Q01` $\in \{7,8\}$ count toward Total Respondents in the denominator.)*

2. **Point-in-Time Labor Cost (`gold_project_profitability`):**
   - Join `silver_timesheets` to `silver_employees_scd2` on:
     `ts.emp_id = emp.emp_id AND ts.work_date >= emp.valid_from AND ts.work_date <= emp.valid_to` (or `< valid_to` depending on half-open vs. closed SCD2 intervals).
   - $\text{labor\_cost\_eur} = \sum (\text{hours} \times \text{hourly\_rate\_eur})$.

3. **Project Revenue & Gross Margin (EUR):**
   - $\text{net\_revenue\_eur} = \text{invoiced\_revenue\_eur} + \text{credit\_notes\_eur}$ *(where `credit_notes_eur` is negative)*, or $\sum \text{amount\_eur}$ across valid non-orphan `silver_invoices`.
   - $\text{gross\_margin\_eur} = \text{net\_revenue\_eur} - \text{labor\_cost\_eur}$.
   - $\text{gross\_margin\_pct} = \frac{\text{gross\_margin\_eur}}{\text{net\_revenue\_eur}} \times 100$ (handle `net_revenue_eur = 0` safely with `NULL`).
   - $\text{budget\_utilization\_pct} = \frac{\text{labor\_cost\_eur}}{\text{budget\_eur}} \times 100$.

4. **Compensation Ratio (`compa_ratio` in `gold_employee_churn_features`):**
   - $\text{compa\_ratio}_i = \frac{\text{hourly\_rate\_eur}_i}{\text{median}(\text{hourly\_rate\_eur} \mid \text{department}, \text{seniority\_level})}$.

5. **Near-Duplicate Tie-Breaker Policy:**
   - When `ts_id` or `exp_id` appears multiple times with slightly differing numeric values, choose a **deterministic** tie-breaker (e.g., lowest valid positive value, highest value, or deterministic hash/order), implement it consistently via `Window.partitionBy(id).orderBy(...)`, and document your choice in `docs/architecture.md`.
