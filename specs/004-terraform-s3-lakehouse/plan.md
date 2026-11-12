# Implementation Plan — Spec 004: Terraform S3 Cloud Infrastructure & `s3://` Lakehouse Backend

**Spec Reference:** [`spec.md`](./spec.md) (`Status: implemented`)  
**Author:** Ronald Solares  
**Date:** 2026-11-11  

---

## 1. Architectural Overview (`s3://` + Local Dual Storage + Terraform IaC)

Instead of rewriting domain models, Pydantic data contracts, or `dbt-duckdb` SQL models, **Spec 004** cleanly encapsulates physical object storage inside `DeltaLakeStorage` and provisions the cloud storage topology declaratively in `infra/terraform/`:

```text
                ┌──► Local Filesystem (`data/lakehouse/<table_name>`) — Default / Pytest
[DeltaLakeStorage]
(delta-rs)      └──► S3 Storage (`s3://<zone-bucket>/<table_name>`)  — Moto S3 Docker ($0) or AWS S3
                        ▲
                        │ Provisioned via Terraform HCL (`infra/terraform/`)
                        ├── 4 Buckets: bronze | silver | dlq | gold
                        ├── Security:  PublicAccessBlock (all true) + Versioning + Lifecycle
                        ├── IAM:       Scoped Lakehouse RW Role & Policy
                        └── Cost:      AWS Budgets Guardrail (`$1.00 USD`, toggleable for AWS vs Local Docker)
```

DuckDB SQL analytics (`dbt-duckdb`) remains fast and portable: `DeltaAnalyticsBridge.hydrate_sources` fetches committed Delta snapshots from `DeltaLakeStorage` (local or `s3://`) via PyArrow and registers `raw_lakehouse.*` into `analytics.duckdb` before `dbt build`.

---

## 2. Key Technical Decisions & Constraints

| Decision | Choice | Alternative Rejected | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **Local S3 Emulator (\$0)** | Slim `Dockerfile.s3-mock` (`moto[s3]>=5.0` on cached Airflow base image, port `5000`) | Heavy `localstack/localstack:3.8.1` (~1.5 GB pull) or `localstack:4+` (mandatory auth token) | Reuses existing local image layers, boots in seconds with full S3 + IAM API compatibility for Terraform `hashicorp/aws`, while `environments/localstack.tfvars` stays intact for LocalStack users. |
| **Terraform Runner** | Official `hashicorp/terraform:1.9` container (`terraform-init` service + `terraform_plugins` cache volume) | Mandatory manual host install | Any contributor with Docker can run `terraform validate/plan/apply` cleanly on Windows or Linux; persistent provider cache avoids re-downloading the AWS provider across runs. |
| **Delta Table S3 Operations** | `delta-rs` native S3 (`deltalake` 1.6.6 `DeltaTable` / `is_deltatable` / `write_deltalake` with `storage_options`) | Adding `boto3` + `s3fs` dependencies | Zero new heavy Python dependencies (`constitution.md` Principle 1); `delta-rs` executes atomic commits/deletes directly against S3. |
| **Multi-Bucket Zone Routing** | Optional `table_uris` / zone-bucket resolver (`LAKEHOUSE_S3_BRONZE_BUCKET`, etc.) + single `s3://` fallback | Shoving all layers into one unpartitioned folder | Mirrors real enterprise Data Lake zone isolation (`bronze`, `silver`, `dlq`, `gold`) matching Terraform bucket outputs. |
| **Budget Resource Toggle** | `enable_budget_guard` variable (`false` in `local-docker.tfvars` & `localstack.tfvars`, `true` in `aws-free-tier.tfvars.example`) | Separate HCL directories for local vs AWS | Zero code fork between \$0 local testing and real AWS Free Plan deployment. |

---

## 3. Repository Structure Additions

```text
delta-settlement-lakehouse/
├── infra/
│   └── terraform/
│       ├── versions.tf                       # Terraform >=1.6 & hashicorp/aws ~>5.0 constraint
│       ├── providers.tf                      # Dynamic AWS vs local S3 endpoint toggle
│       ├── variables.tf                      # Prefix, environment, retention days, endpoint & budget vars
│       ├── main.tf                           # 4 Medallion S3 buckets, PublicAccessBlock, Versioning, Lifecycle, IAM, Budget
│       ├── outputs.tf                        # Bucket names, ARNs, zone S3 URIs, IAM role ARN
│       └── environments/
│           ├── local-docker.tfvars           # Default $0 local Compose Moto (`s3-mock:5000`) config
│           ├── localstack.tfvars             # Optional $0 LocalStack (`localstack:4566`) defaults
│           └── aws-free-tier.tfvars.example  # Real AWS Free Plan deployment template
├── delta_settlement_lakehouse/
│   ├── engines/
│   │   ├── delta_storage.py                  # StorageOptions/zone-URI routing, S3 is_deltatable, delete_where
│   │   └── dbt_bridge.py                     # Forward S3 storage_options & zone buckets cleanly
│   └── orchestration/
│       ├── assets.py                         # Dagster assets supporting optional S3 config
│       └── tasks.py                          # Shared tasks using storage.delete_where() & S3 config from env
├── airflow/dags/settlement_medallion_dag.py  # Propagate optional S3 config across TaskFlow tasks
├── Dockerfile.s3-mock                        # Lightweight local Moto S3+IAM endpoint container
├── docker-compose.yml                        # Opt-in `cloud` profile: s3-mock + terraform-init + airflow-cloud
├── .env.example & .gitignore                 # Ignore *.tfstate*, .terraform/; document S3 env vars
└── tests/
    ├── test_s3_storage_backend.py            # TDD tests for S3 URI resolution, storage_options, zone routing
    └── test_terraform_infra.py               # TDD contract tests for Terraform HCL, Compose `cloud` profile & DAG S3 config
```

---

## 4. Verification Strategy

1. **Spike `T0` (Container Frontier):** Verify `docker desktop start`, boot `settlement-lakehouse-s3-mock:5.x` (`moto[s3]`) + `hashicorp/terraform:1.9`, and validate `deltalake 1.6.6` `write_deltalake` + `DeltaTable.is_deltatable` + `DeltaTable.delete` over `s3://` (`AWS_S3_ALLOW_UNSAFE_RENAME=true`, `AWS_ALLOW_HTTP=true`).
2. **TDD Vertical Slices (`T1`–`T3`):**
   - `T1`: `DeltaLakeStorage` + `orchestration/tasks.py` + `dbt_bridge.py` S3 dual-backend (`RF-1`, `RF-2`, `RF-3`) + Red/Green unit tests (`test_s3_storage_backend.py`), ensuring existing 32 tests remain 100% green.
   - `T2`: `infra/terraform/` HCL files + `.gitignore` updates (`RF-4`, `RF-5`) + Terraform HCL contract tests (`test_terraform_infra.py`). Validate cleanly via `hashicorp/terraform` container (`terraform init` + `terraform validate`).
   - `T3`: Airflow DAG + `docker-compose.yml` `cloud` profile (`s3-mock`, `terraform-init`, `airflow-cloud`) + Compose/DAG tests (`RF-6`).
3. **Live Container End-to-End Verification (`T4`):**
   - Start `docker compose --profile cloud up -d postgres s3-mock` -> run `airflow-init` -> run `terraform-init` twice (19 resources applied + idempotent 0-change rerun) -> run `airflow-cloud airflow dags test settlement_medallion_dag 2026-10-28` twice -> verify `bronze=100`, `silver=88`, `dlq=12`, `gold=3` (`GoldService.reconcile_date`) & `duckdb_tx=88` (`PASS=30` dbt tests) + S3 versioning/lifecycle/public-access blocks & IAM role -> cleanly stop containers (`down --remove-orphans`, preserving `terraform_plugins` cache volume).
4. **Docs, Push & `job_copilot` Upgrade (`T5`):** Update `README.md`, `MEMORY.md`, push `delta-settlement-lakehouse`, then TDD-add `Terraform (IaC)` and `AWS S3 (Data Lake)` into `job_copilot` (`config.py`, `analyzer.py`, `cv_renderer.py`, `catalog.py`, `post_generator.py`, `MEMORY.md`, and tests), verify `py -m pytest job_copilot/tests -q` green, and push.

