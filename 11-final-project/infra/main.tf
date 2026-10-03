# Least-privilege role for the dashboard: read the marts and the alerts, nothing else.
# Reuses the module from the repository's infra/ folder (one module, two projects).
terraform {
  required_version = ">= 1.9"
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.27"
    }
  }
}

variable "pg_host" {
  type    = string
  default = "localhost"
}

variable "pg_port" {
  type    = number
  default = 5435
}

variable "pg_admin_password" {
  type      = string
  sensitive = true
}

variable "dashboard_password" {
  type      = string
  sensitive = true
}

provider "postgresql" {
  host      = var.pg_host
  port      = var.pg_port
  database  = "warehouse"
  username  = "de"
  password  = var.pg_admin_password
  sslmode   = "disable"
  superuser = false
}

resource "postgresql_schema" "readable" {
  for_each = toset(["velib_marts", "monitoring"])
  name     = each.key
  database = "warehouse"
}

module "dashboard" {
  source           = "../../infra/modules/warehouse_role"
  name             = "velib_dashboard"
  password         = var.dashboard_password
  database         = "warehouse"
  schema_owner     = "de"
  read_schemas     = [for s in postgresql_schema.readable : s.name]
  write_schemas    = []
  connection_limit = 5
}

output "dashboard_role" {
  value = module.dashboard.role_name
}
