variable "environment" {
  description = "dev or prod: prefixes every role so both can live in one cluster"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
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

variable "pg_database" {
  description = "The warehouse database of projects 03 and 04"
  type        = string
  default     = "de"
}

variable "pg_admin_user" {
  type    = string
  default = "de"
}

variable "pg_admin_password" {
  description = "Never committed: pass it with TF_VAR_pg_admin_password"
  type        = string
  sensitive   = true
}

variable "analyst_password" {
  type      = string
  sensitive = true
}

variable "loader_password" {
  type      = string
  sensitive = true
}
