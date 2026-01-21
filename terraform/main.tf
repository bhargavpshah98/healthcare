data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  raw_bucket_name       = "${var.project_name}-raw-${local.account_id}"
  processed_bucket_name = "${var.project_name}-processed-${local.account_id}"
}
