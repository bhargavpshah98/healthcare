resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "glue.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

# Base Glue service role permissions (CloudWatch logs, metrics, etc.)
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 access for your specific buckets (least privilege for this assignment)
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.project_name}-glue-s3-policy"
  description = "Allow Glue job to read raw bucket and write processed bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "ListBuckets",
        Effect = "Allow",
        Action = ["s3:ListBucket"],
        Resource = [
          aws_s3_bucket.raw.arn,
          aws_s3_bucket.processed.arn
        ]
      },
      {
        Sid    = "ReadRawObjects",
        Effect = "Allow",
        Action = ["s3:GetObject"],
        Resource = [
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Sid    = "WriteProcessedObjects",
        Effect = "Allow",
        Action = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
        Resource = [
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}
