output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "glue_job_name" {
  value = aws_glue_job.healthcare_etl.name
}
