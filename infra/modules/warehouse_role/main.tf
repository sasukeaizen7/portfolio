# A login role with USAGE + read (SELECT) on some schemas and read/write on others, including tables
# created LATER (default privileges): without them, every new dbt table would need a manual GRANT.
terraform {
  required_providers {
    postgresql = {
      source = "cyrilgdn/postgresql"
    }
  }
}

resource "postgresql_role" "this" {
  name             = var.name
  login            = true
  password         = var.password
  connection_limit = var.connection_limit
}

resource "postgresql_grant" "usage" {
  for_each    = toset(concat(var.read_schemas, var.write_schemas))
  database    = var.database
  role        = postgresql_role.this.name
  schema      = each.key
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "read_tables" {
  for_each    = toset(var.read_schemas)
  database    = var.database
  role        = postgresql_role.this.name
  schema      = each.key
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "read_future_tables" {
  for_each    = toset(var.read_schemas)
  database    = var.database
  role        = postgresql_role.this.name
  owner       = var.schema_owner
  schema      = each.key
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_grant" "write_tables" {
  for_each    = toset(var.write_schemas)
  database    = var.database
  role        = postgresql_role.this.name
  schema      = each.key
  object_type = "table"
  privileges  = ["SELECT", "INSERT", "UPDATE", "DELETE"]
}

resource "postgresql_default_privileges" "write_future_tables" {
  for_each    = toset(var.write_schemas)
  database    = var.database
  role        = postgresql_role.this.name
  owner       = var.schema_owner
  schema      = each.key
  object_type = "table"
  privileges  = ["SELECT", "INSERT", "UPDATE", "DELETE"]
}
