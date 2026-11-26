# rygrante_liga_bd

Team repository for the Spark Lakehouse course project, based on [smoothgrinder/spark-lakehouse-starter](https://github.com/smoothgrinder/spark-lakehouse-starter).

## CorpTech Enterprise Lakehouse & Medallion Pipeline (Starter)

Student starter repository for the **Apache Spark Lakehouse & Medallion Architecture** course project.

- **Assignment Specification:** [`docs/STUDENT_ASSIGNMENT.md`](./docs/STUDENT_ASSIGNMENT.md)
- **14-Day Data Sanity Checks & KPI Glossary:** [`docs/SANITY_CHECKS.md`](./docs/SANITY_CHECKS.md)
- **Architecture Design Template (Student Deliverable):** [`docs/architecture.md`](./docs/architecture.md)
- **Operations Runbook (Student Deliverable):** [`docs/runbook.md`](./docs/runbook.md)

---

## Repository Layout

```text
spark-lakehouse-starter/
├── .github/workflows/ci.yml      # Out-of-the-box CI & security workflow (uv, pytest, Gitleaks, Semgrep, Trivy)
├── .gitleaks.toml                # Gitleaks secret scan configuration
├── .semgrepignore                # Semgrep OSS exclusion list
├── docker/
│   ├── airflow/Dockerfile        # Airflow 2.10.5 (LocalExecutor) + PySpark 3.5.5 + Delta JARs
│   ├── reporting/Dockerfile      # Streamlit + DuckDB (with pre-installed delta extension)
│   └── spark/Dockerfile          # Spark 3.5.5 + Delta Lake 3.3.0 + Hadoop-AWS 3.3.4 JARs
├── docker-compose.yml            # Reproducible stack (+ optional --profile minio)
├── pyproject.toml                # Python dependency specification & pytest/ruff config
├── uv.lock                       # Reproducible dependency lockfile
├── .env.example                  # Environment variable template
├── scripts/
│   ├── download_data.py          # Manifest-driven Cloudflare R2 downloader & verifier
│   ├── download_data.sh          # Linux/macOS wrapper (uses uv / python3 / docker)
│   └── download_data.ps1         # Windows PowerShell wrapper (uses uv / python / docker)
├── dags/
│   └── medallion_daily_dag.py    # Parameterized Airflow DAG (`clients` wired + TODO tasks)
├── spark_jobs/
│   ├── common/spark_session.py   # Shared Delta/S3A SparkSession builder & path helpers
│   ├── bronze/                   # Reference `ingest_clients.py` + TODO skeletons
│   ├── silver/                   # Reference `transform_clients.py` + TODO skeletons
│   └── gold/                     # TODO skeletons for the 5 Gold marts
├── dq/
│   ├── framework.py              # `DQCheckResult`, `dq_audit_results` Delta writer, gate runner
│   └── checks.py                 # TODO skeletons for full Bronze/Silver/Gold DQ suite
├── reporting/
│   ├── db.py                     # DuckDB `delta_scan` helper
│   └── app.py                    # Streamlit Home status page + 4 TODO view tabs
├── tests/
│   ├── conftest.py               # PySpark local session fixture
│   ├── test_clients_pipeline.py  # Working tests for reference `clients` Bronze/Silver & DQ audit
│   └── test_download_data.py     # Unit tests for manifest-driven R2 downloader & verification
└── docs/
    ├── STUDENT_ASSIGNMENT.md
    ├── SANITY_CHECKS.md
    ├── architecture.md
    └── runbook.md
```

---

## Quickstart

Commands are given for **macOS**, **Windows (PowerShell)**, and **Linux (Arch / Debian / Ubuntu)**. Where only one block is shown, it works the same on every OS. Run everything from the repository root.

### 1. Prerequisites

You need **Git**, **Docker with Compose v2** (`docker compose`, not `docker-compose`), and **`uv`**. Give Docker at least **8 GB RAM** and keep **~20 GB** free disk.

**macOS** (Homebrew):
```bash
brew install git uv
brew install --cask docker   # then open Docker Desktop once; set Memory >= 8 GB in Settings > Resources
```

**Windows** (PowerShell; run `wsl --install` as Administrator first and reboot if asked):
```powershell
winget install --id Git.Git -e --source winget
winget install --id Docker.DockerDesktop -e --source winget
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Start Docker Desktop (WSL 2 backend). On the WSL 2 backend, RAM is set in `%UserProfile%\.wslconfig` (`[wsl2]` then `memory=8GB`), not in Docker Desktop settings.

**Arch Linux:**
```bash
sudo pacman -Syu --needed git docker docker-compose docker-buildx uv
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"   # log out and back in, so docker works without sudo
```
*(If `pacman` fails with `404` while downloading, your package database is stale: the `-Syu` above refreshes it. Reboot after a kernel upgrade, otherwise containers fail with `veth ... operation not supported`.)*

**Debian / Ubuntu:** install Docker Engine + the Compose plugin from [docs.docker.com/engine/install](https://docs.docker.com/engine/install/), then:
```bash
sudo apt-get install -y git
curl -LsSf https://astral.sh/uv/install.sh | sh
sudo usermod -aG docker "$USER"   # log out and back in
```

Then, on every OS, open a new terminal and verify:
```bash
git --version
docker --version
docker compose version
uv --version
uv python install 3.11
```
*(On Linux, Docker Engine runs as a system service, so there is no Docker Desktop window to open. `systemctl is-active docker` should print `active`.)*

> **Windows Note:** Running Spark and `pytest` inside the provided Docker containers avoids local Hadoop `winutils.exe` and JVM version issues.

### 2. Configure `.env`

Create `.env` from the template. These commands never overwrite an existing `.env`, so your credentials are safe if you run them again.

**macOS / Linux:**
```bash
cp -n .env.example .env
```

**Windows (PowerShell):**
```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Fill in the `R2_*` variables provided by your instructor. Never commit `.env`: it is in `.gitignore`, and CI runs Gitleaks.

### 3. Download & Verify the Landing Dataset
The course dataset contains **200 development days** (`2026-01-01` – `2026-07-19`) distributed via **Cloudflare R2** using a SHA-256 manifest (`MANIFEST.sha256`) under `R2_PREFIX=course-2026`. *(An additional **70-day holdout**, `2026-07-20` – `2026-09-27`, is reserved for final grading and out-of-time evaluation.)*

**macOS / Linux / Git Bash:**
```bash
./scripts/download_data.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\download_data.ps1
```

*(Both wrappers invoke `scripts/download_data.py`, which reads `MANIFEST.sha256` from Cloudflare R2, downloads and SHA-256-verifies the listed archive before extracting it into `./data/landing`, and checks all 10 required dataset folders. Pass `--verify-only` / `-VerifyOnly` to re-verify locally without downloading, or `--url` / `-Url` for a direct HTTP mirror.)*

> **`NoSuchKey` error?** If the log shows `Using manifest .../course-2026/archives/MANIFEST.sha256` and then fails with `NoSuchKey`, the archive lives under `archives/`. Set `R2_PREFIX=course-2026/archives` in `.env` and run the download again (or pass `--r2-prefix course-2026/archives` / `-R2Prefix course-2026/archives` once).

If PowerShell refuses to run the script (`running scripts is disabled on this system`), allow local scripts for your user once:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 4. Start the Local Stack

**Linux only (first run):** the containers run as different users than you (Spark as UID `185`, Airflow as UID `50000`). Create the bind-mounted folders yourself and make them writable, otherwise Docker creates them as `root` and Spark History / Airflow crash with `Permission denied`:
```bash
mkdir -p data/landing lakehouse/spark-events logs
chmod 777 data lakehouse logs dags
chmod 755 data/landing
```
Keep `AIRFLOW_UID=50000` in `.env`. Setting it to your own UID (`id -u`) breaks `airflow-init` with `The user that Airflow is running as has no username`.

*(macOS and Windows: Docker Desktop handles file ownership, so skip this step.)*

**Every OS:**
```bash
docker compose up -d --build
docker compose ps
```
The first build takes several minutes. All services should be `Up`; `airflow-init` should show `Exited (0)`.
*(To also start a local S3-compatible MinIO server, run `docker compose --profile minio up -d --build`.)*

### 5. Run the Reference `clients` Mini-Pipeline
These are single-line commands, so they work unchanged in bash, zsh, and PowerShell:
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/workspace/spark_jobs/bronze/ingest_clients.py --batch-date 2026-01-01
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/workspace/spark_jobs/silver/transform_clients.py --batch-date 2026-01-01
```
*(If the Bronze job fails with `PATH_NOT_FOUND ... /opt/workspace/data/landing/clients/clients.csv` on Linux, run `chmod 755 data/landing`: the downloader can create it readable by you only.)*

### 6. Run Unit Tests
```bash
docker compose run --rm spark-master pytest -v
```

### 7. Stop the Stack
```bash
docker compose down
```
*(This keeps your built images and the Airflow database. `docker compose down -v` also deletes the database volume.)*

### Troubleshooting by OS

| Symptom | OS | Fix |
| :--- | :--- | :--- |
| `permission denied ... /var/run/docker.sock` | Linux | `sudo usermod -aG docker "$USER"`, then log out and back in |
| `Cannot connect to the Docker daemon` | macOS / Windows | Start Docker Desktop and wait until it says *running* |
| `Cannot connect to the Docker daemon` | Linux | `sudo systemctl start docker` |
| `compose build requires buildx` | Linux | Arch: `sudo pacman -S docker-buildx` · Debian/Ubuntu: `sudo apt-get install docker-buildx-plugin` |
| `veth ... operation not supported` | Linux | Kernel was upgraded but not rebooted: reboot |
| `Permission denied` in `spark-history` / `airflow-*` logs | Linux | Run the step 4 folder commands, then `docker compose up -d` |
| `$'\r': command not found` when running `.sh` scripts | Windows | Line endings were converted to CRLF: `git config --global core.autocrlf input`, then re-clone |
| `running scripts is disabled on this system` | Windows | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Containers get killed / Spark `OutOfMemory` | All | Give Docker more RAM (macOS: Docker Desktop settings · Windows: `.wslconfig`) and close other heavy apps |

---

## CI & Security Checks

An out-of-the-box GitHub Actions workflow ([`.github/workflows/ci.yml`](./.github/workflows/ci.yml)) runs automatically on pushes and pull requests to `main` and `develop` without requiring paid tokens or external SaaS licenses:

| Job | Checks |
| :--- | :--- |
| **`lint-and-uv`** | `uv lock --check`, `uv sync --frozen --dev`, `ruff check .`, `ruff format --check .`, `bash -n scripts/download_data.sh`, `docker compose config --quiet` |
| **`unit-tests`** | `uv run pytest -v` on Python 3.11 + Temurin Java 17 |
| **`gitleaks`** | Full Git history secret scan via OSS Gitleaks binary ([`.gitleaks.toml`](./.gitleaks.toml)) |
| **`semgrep`** | OSS SAST across Python, Dockerfiles, Compose, and secrets (`semgrep scan`) |
| **`trivy`** | `CRITICAL,HIGH` vulnerability, secret, and Dockerfile/Compose IaC scans (`aquasecurity/trivy-action`) |

Run the local `uv` and Ruff checks before committing:
```bash
uv lock --check
uv run --no-project --with ruff ruff check .
uv run --no-project --with ruff ruff format --check .
```

### Run CI locally with `make`

The [`Makefile`](./Makefile) mirrors every CI job, so you can reproduce a red pipeline on your laptop before pushing. Security scanners run in Docker, so nothing extra needs installing (Windows: use Git Bash or WSL).

| Command | What it runs |
| :--- | :--- |
| `make` | List all targets |
| `make ci-fast` | Lint + tests — **run before every push** |
| `make ci` | All 5 CI jobs (lint, tests, Gitleaks, Semgrep, Trivy) |
| `make lint` / `make fix` | Check / auto-fix Ruff lint and formatting |
| `make test` | `pytest` inside the `spark-master` container |
| `make gitleaks` / `make semgrep` / `make trivy` | Individual security scans |
| `make up` / `make down` / `make ps` / `make logs s=<service>` | Manage the Docker stack |

`make trivy` skips `.env`, `data/`, `lakehouse/` and `logs/`, because CI never sees those local-only files.

#### Per-OS runners

All three runners have the same CI and Docker targets. Use the one for your OS:

| OS | Runner | Example |
| :--- | :--- | :--- |
| macOS | [`Makefile`](./Makefile) | `make ci-fast` |
| Linux (Arch) | [`MakefileL`](./MakefileL) (includes `Makefile`) | `make -f MakefileL ci-fast` |
| Windows | [`MakefileW`](./MakefileW) (runs [`make.ps1`](./make.ps1)) | `make -f MakefileW ci-fast` |

Extra setup targets for a fresh clone:

| Target | Linux (`make -f MakefileL …`) | Windows (`make -f MakefileW …`) |
| :--- | :--- | :--- |
| `install` | `pacman` packages, enable Docker, join `docker` group (sudo) | not available: use `winget` (see Quickstart) |
| `setup` | Creates `.env` (never overwrites), fixes folder permissions, `uv sync` | Creates `.env` (never overwrites), `uv sync` |
| `dirs` | Creates writable `data/`, `lakehouse/`, `logs/` for the containers | not needed (Docker Desktop) |
| `data` | Downloads + verifies the dataset, `chmod 755 data/landing` | Downloads + verifies the dataset |
| `first-run` | `setup` + `data` + `build` + `test` | `setup` + `data` + `build` + `test` |
| `doctor` | Checks kernel/reboot, Docker service, `docker` group, buildx, folders, `.env` | Checks Docker Desktop, WSL, uv, git `autocrlf`, `.env`, dataset |
| `java` | Installs Java 17 for `test-host` | not available |

On Linux, set `alias lmake='make -f MakefileL'` once, then use `lmake doctor`. On Windows, install GNU make once with `winget install ezwinports.make`, then open a new terminal. Without make, run the same targets directly with `.\make.ps1 <target>`, using `-Service <name>` instead of `s=<name>` for `logs`. If PowerShell blocks the script, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.

---

## Local Service URLs

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Apache Airflow** | http://localhost:8080 | `admin` / `admin` |
| **Spark Master UI** | http://localhost:8081 | — |
| **Spark Worker UI** | http://localhost:8082 | — |
| **Spark History Server** | http://localhost:18080 | — |
| **Streamlit Reporting UI** | http://localhost:8501 | — |
| **MinIO Console** *(optional)* | http://localhost:9001 | `minioadmin` / `minioadmin` |

---

## What Is Provided vs. What You Must Implement

### Provided in Starter
- Dockerized **Spark 3.5.5 + Delta Lake 3.3.0 + Airflow 2.10.5 + DuckDB/Streamlit** environment (with JARs pre-baked for offline execution)
- Manifest-driven Cloudflare R2 dataset download & verification scripts (`scripts/download_data.{py,sh,ps1}`)
- Shared `SparkSession` builder (`spark_jobs/common/spark_session.py`)
- Reference Bronze → Silver mini-pipeline for **`clients`** (`ingest_clients.py` & `transform_clients.py`), `dq_audit_results` schema/writer (`dq/framework.py`), and unit tests (`tests/test_clients_pipeline.py`)
- Parameterized Airflow DAG skeleton (`dags/medallion_daily_dag.py`) and Streamlit overview skeleton (`reporting/app.py`)

### Student Deliverables (`TODO`)
1. **Bronze Ingestion:** Implement `ingest_reference.py`, `ingest_hr.py`, `ingest_finance.py`, and `ingest_crm_surveys.py` with idempotent `_batch_date` partitioning.
2. **Silver Transformations & Quarantine:** Implement `employees` SCD Type 2, `timesheets` dedup/quarantine, `fx_rates` calendar spine + LOCF, `invoices` & `expenses` EU date parsing + FX normalization + orphan quarantine, `crm_events` skew-mitigated join (with Spark UI evidence in `docs/architecture.md`), and `employee_surveys` explode + bounds checks.
3. **Gold Data Marts:** Implement all 5 Gold tables (`gold_project_profitability`, `gold_department_finance`, `gold_client_engagement`, `gold_hr_enps_analytics`, `gold_employee_churn_features`).
4. **Data Quality Gates:** Implement full Hard/Soft checks in `dq/checks.py` and wire `dq_*_gate` tasks in Airflow.
5. **Orchestration & Reporting:** Wire all tasks in `dags/medallion_daily_dag.py`, build the 4 views in `reporting/app.py`, add tests in `tests/`, and complete `docs/architecture.md` and `docs/runbook.md`.
