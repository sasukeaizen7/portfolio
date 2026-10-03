output "roles" {
  description = "Roles managed by this configuration"
  value       = [module.analyst.role_name, module.loader.role_name]
}

output "analyst_dsn" {
  description = "Connection string for the analyst role"
  value       = "postgresql://${module.analyst.role_name}:${var.analyst_password}@${var.pg_host}:${var.pg_port}/${var.pg_database}"
  sensitive   = true
}
