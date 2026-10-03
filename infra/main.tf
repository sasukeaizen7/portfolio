# Infrastructure as code for the warehouse of projects 03 (raw/dw/audit) and 04 (dbt_* schemas):
# least-privilege roles instead of everyone using the owner account.
#   * analyst: read-only on the modeled layers (dw, dbt_marts), nothing on raw;
#   * loader:  writes the raw layer (what Project 1's pipeline needs), reads nothing else.
provider "postgresql" {
  host            = var.pg_host
  port            = var.pg_port
  database        = var.pg_database
  username        = var.pg_admin_user
  password        = var.pg_admin_password
  sslmode         = "disable"
  connect_timeout = 15
  superuser       = false
}

locals {
  prefix = "${var.environment}_"
}

# Schemas the roles are granted on. Terraform creates them if missing and leaves their tables alone
# (the pipelines own the tables; Terraform owns access).
resource "postgresql_schema" "layer" {
  for_each = toset(["raw", "dw", "dbt_marts"])
  name     = each.key
  database = var.pg_database
}

module "analyst" {
  source           = "./modules/warehouse_role"
  name             = "${local.prefix}analyst"
  password         = var.analyst_password
  database         = var.pg_database
  schema_owner     = var.pg_admin_user
  read_schemas     = [postgresql_schema.layer["dw"].name, postgresql_schema.layer["dbt_marts"].name]
  write_schemas    = []
  connection_limit = 10
}

module "loader" {
  source           = "./modules/warehouse_role"
  name             = "${local.prefix}loader"
  password         = var.loader_password
  database         = var.pg_database
  schema_owner     = var.pg_admin_user
  read_schemas     = []
  write_schemas    = [postgresql_schema.layer["raw"].name]
  connection_limit = 4
}
