variable "name" {
  type = string
}

variable "password" {
  type      = string
  sensitive = true
}

variable "database" {
  type = string
}

variable "schema_owner" {
  description = "Role that creates the tables (default privileges apply to tables it creates)"
  type        = string
}

variable "read_schemas" {
  type    = list(string)
  default = []
}

variable "write_schemas" {
  type    = list(string)
  default = []
}

variable "connection_limit" {
  type    = number
  default = 5
}
