# infra · Terraform: least-privilege access to the warehouse

*Plan day 82: Terraform core sections.*

Terraform manages **who can do what** in the Postgres warehouse used by projects [03](../03-project-1-ingestion) and [04](../04-dbt-analytics). The pipelines own the tables; Terraform owns the access.

| Role | Can | Cannot |
|---|---|---|
| `dev_analyst` | read `dw` and `dbt_marts`, including tables created later | read `raw`, write anything |
| `dev_loader` | read/write `raw` (what Project 1's loader needs) | read `dw` or the marts |

## What it demonstrates
- **Providers, variables, outputs:** sensitive variables are passed through `TF_VAR_*` and never committed, and `validation` blocks reject bad environments.
- **A reusable module** (`modules/warehouse_role`): one definition and two roles with different permissions.
- **`for_each`** over schemas, and dependencies through references.
- **Default privileges:** tables that dbt creates *after* the apply are readable without a manual `GRANT`. That's the part people forget.
- **Environments:** `environment = "prod"` creates `prod_*` roles next to `dev_*` ones. Use separate state files (or workspaces) per environment.
- **Idempotency:** a second `plan` after `apply` reports no changes. CI runs `fmt -check` and `validate`.

## Run it
With Project 1's warehouse running on localhost:5435:
```bash
cp terraform.tfvars.example terraform.tfvars        # then set the passwords
```
```bash
terraform init && terraform plan && terraform apply
```
```bash
pytest tests          # checks every allowed and denied action, as the real roles (needs the TF_VAR_* passwords)
```
Result here: 14 resources created, re-plan clean, 3/3 grant tests passing.

## Exercises
1. Add a `dbt` role that can create tables in the `dbt_*` schemas but not touch `raw`, and run dbt as that role.
2. Move state to a remote backend (S3 + locking) and explain why local state doesn't work for a team.
3. Add `prevent_destroy` to the schemas and see what `terraform destroy` does.
4. Write the AWS equivalent of this access model: an IAM role, an S3 bucket policy for the raw zone, and a Glue catalog resource policy.
