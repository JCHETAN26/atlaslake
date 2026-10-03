# AtlasLake on AWS (Phases 31–32)

AtlasLake is developed and benchmarked on the local Docker stack. AWS hosts a **production-like
slice** for cloud validation. It is all Terraform, and it costs close to nothing until you switch a
service on. This is deliberate. We don't need to spend hundreds of dollars to prove we know AWS.

## What runs where

| Component | Local (docker-compose) | AWS (Terraform) |
|---|---|---|
| Operational source DB | Postgres 16 container | **RDS PostgreSQL 16** (`db.t4g.micro` dev), private subnets, KMS-encrypted, `rds.force_ssl=1` |
| Lake storage (Bronze/Silver/Gold/Iceberg) | MinIO | **S3** bucket, SSE-KMS (CMK), versioned, TLS-only policy, lifecycle |
| Iceberg catalog | JDBC catalog in Postgres | same JDBC catalog, in RDS (`iceberg_catalog` DB) |
| Kafka | `apache/kafka` KRaft | **MSK Serverless** (IAM auth + TLS) when `enable_msk=true`; otherwise self-managed Kafka reachable from the VPC (`external_kafka_bootstrap`) |
| Schema Registry | Confluent SR container | runs next to Kafka (`schema_registry_url`) |
| Outbox relay | `atlas relay --follow` | **ECS Fargate** service `relay` |
| Spark streaming | local `local[*]` process | **ECS Fargate** service `stream` (Spark `local[*]` in one task) |
| Container images | local build | **ECR** `atlaslake-app`, `atlaslake-airflow` (immutable tags, scan on push, KMS) |
| Logs / metrics / alerts | Prometheus, Grafana, Alertmanager | **CloudWatch** log groups (14 d), metric filters, alarms → **SNS** |
| dbt Gold / Snowflake publish / Airflow | local | run from the same image as ad-hoc ECS tasks or from Airflow (out of scope for this slice) |

### Why Spark runs as a single Fargate task

The streaming job processes one checkpoint with one driver. At the volumes we benchmark (see
`evaluation/`), one task with up to 4 vCPU / 16 GB (Fargate) running `local[*]` handles the load.
EMR or EMR Serverless would add a fixed cost and more moving parts for no gain we can measure. The
Terraform enforces `stream_desired_count <= 1` because two drivers on one checkpoint would corrupt
it. If a benchmark ever shows a single task is the bottleneck, the next step is EMR Serverless with
the same job code, since nothing in `spark/` is tied to local mode.

### Why Kafka is optional

MSK Serverless costs about $0.75 per cluster-hour (roughly $550/month) before throughput charges.
That's far more than everything else combined. So `enable_msk` defaults to `false`, and the env
points at an external broker instead. Turn it on for a time-boxed cloud validation run, then off
again. With MSK on, the task role gets `kafka-cluster:*` permissions scoped to the cluster,
`commerce.*` topics and the consumer groups, and clients use IAM auth over TLS (port 9098).

## Layout

```
infrastructure/terraform/
├── modules/
│   ├── networking/   VPC, private subnets (no NAT), S3 gateway endpoint, optional interface endpoints
│   ├── security/     KMS CMK (rotation on), security groups (ECS → RDS/MSK only, no inbound to tasks)
│   ├── storage/      lake bucket + policies + lifecycle, ECR repositories
│   ├── database/     RDS Postgres, parameter group (force_ssl), Secrets Manager secret (random password)
│   ├── compute/      ECS cluster, task/execution roles (least privilege), task definitions, services, optional MSK
│   └── monitoring/   SNS topic, RDS/ECS alarms, log metric filters, consumer-lag alarm
└── environments/
    ├── dev/          single-instance RDS, 1-day backups, no deletion protection, endpoints off by default
    └── prod-like/    Multi-AZ RDS (m7g.large), 7-day backups, deletion protection, 3 AZs, Container Insights
```

## Cost estimate (us-west-2 list prices, approximate, per month)

| Item | dev, all services at `desired_count = 0` | dev, relay + stream running 24/7 |
|---|---:|---:|
| RDS `db.t4g.micro` + 20 GB gp3 | ~$14 | ~$14 |
| KMS CMK | $1 | $1 |
| Secrets Manager (1 secret) | $0.40 | $0.40 |
| S3 (≈10 GB lake) + requests | ~$1 | ~$2–5 |
| CloudWatch alarms (~6) + logs | ~$1 | ~$3–10 |
| ECR storage (≈2 GB) | $0.20 | $0.20 |
| Interface VPC endpoints (7 × 2 AZ) | $0 (off) | ~$100 |
| Fargate `relay` (0.5 vCPU / 1 GB) | $0 | ~$18 |
| Fargate `stream` (2 vCPU / 8 GB) | $0 | ~$85 |
| MSK Serverless (`enable_msk`) | $0 (off) | +~$550 if enabled |
| **Total** | **≈ $18** | **≈ $225 (+ MSK if enabled)** |

A typical validation run is "on for a few hours, then off". That costs a few dollars.
`terraform destroy` on dev removes everything (`force_destroy = true` for the bucket and ECR repos).

## Deploy

Prerequisites: AWS credentials for a sandbox account, Terraform ≥ 1.6 (or the
`hashicorp/terraform` image), and Docker.

```bash
cd infrastructure/terraform/environments/dev
cp terraform.tfvars.example terraform.tfvars        # set region, alert_emails, Kafka choice
terraform init                                       # (optionally enable the S3 backend in versions.tf)
terraform plan -out tfplan
terraform apply tfplan                               # creates everything with services at 0 tasks

# build + push the application image (immutable tags)
REPO=$(terraform output -json ecr_repositories | jq -r '."atlaslake-app"')
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%/*}"
docker build -f docker/app/Dockerfile -t "$REPO:v0.1.0" ../../../..
docker push "$REPO:v0.1.0"

# initialise schemas/topics/tables once (one-off task with the same image), then switch services on
terraform apply -var app_image_tag=v0.1.0 -var enable_interface_endpoints=true \
                -var relay_desired_count=1 -var stream_desired_count=1
# ... validate ... then switch off again
terraform apply -var relay_desired_count=0 -var stream_desired_count=0 -var enable_interface_endpoints=false
```

CI runs `terraform fmt -check -recursive` and `terraform validate` for both environments on every
push. It never runs `plan`/`apply` (no cloud credentials live in CI).

## Known gaps (deliberate)

- **S3 credentials:** locally the app passes `ATLAS_S3_ACCESS_KEY`/`ATLAS_S3_SECRET_KEY` to MinIO.
  On AWS those env vars are left unset, so the AWS SDK default chain picks up the ECS task role.
  Static keys are never put in task definitions.
- **Consumer-lag alarm:** the streaming job computes `atlas_stream_consumer_lag` as a Prometheus
  metric. In AWS it reaches CloudWatch (namespace `AtlasLake`) via the CloudWatch agent/ADOT
  scraping the job, or via `PutMetricData`, which the task role is allowed to call for that
  namespace only. Until then the alarm treats missing data as OK.
- **Airflow:** it stays local or on a VM. MWAA's minimum price is not justified for this project.
