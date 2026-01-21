# Healthcare ETL Pipeline (AWS + Terraform)

## Overview
This project implements a simple end-to-end ETL pipeline using AWS services and Terraform.

The pipeline:
- Extracts a public CSV dataset
- Stores it in Amazon S3 (raw layer)
- Transforms the data using AWS Glue (PySpark)
- Writes optimized Parquet output to S3 (processed layer)

## Architecture
S3 (raw) → AWS Glue Job → S3 (processed)

## Tech Stack
- AWS S3
- AWS Glue (Spark, Python)
- AWS IAM
- Terraform
- AWS CLI

## Buckets
- Raw bucket: `healthcare-raw-<account-id>`
- Processed bucket: `healthcare-processed-<account-id>`

## Dataset
This project uses publicly available healthcare insurance data published by
the U.S. Centers for Medicare & Medicaid Services (CMS).

Dataset: Medicare Physician & Other Practitioners - by Provider and Service (2023)

Source: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service

**Note**: The CSV file is not included in this repository due to size constraints (GitHub limits files to 100MB). You must download it separately.

Download the CSV file from the source link above (select "Download" or export as CSV). Save it as `data/medicare_claims.csv` in the project root.

## Deploy / Run

### Prerequisites
- **AWS account** with permissions to create S3 buckets, IAM roles/policies, and Glue jobs.
- **Terraform** \(>= 1.5\)
- **AWS CLI** configured (e.g., `aws configure`) for the target account/region.

> Note: Terraform in this repo provisions resources in **us-east-1** (`terraform/providers.tf`).

### 1) Provision infrastructure (Terraform)
From the repo root:

```bash
cd terraform
terraform init
terraform apply
```

After apply, capture outputs:

```bash
RAW_BUCKET="$(terraform output -raw raw_bucket_name)"
PROCESSED_BUCKET="$(terraform output -raw processed_bucket_name)"
GLUE_JOB_NAME="$(terraform output -raw glue_job_name)"
echo "RAW_BUCKET=$RAW_BUCKET"
echo "PROCESSED_BUCKET=$PROCESSED_BUCKET"
echo "GLUE_JOB_NAME=$GLUE_JOB_NAME"
```

### 2) Upload the Glue script and input data to S3
The Glue job is configured to load its script from:
- `s3://$RAW_BUCKET/scripts/transform.py`

And the ETL script reads input from:
- `s3://$RAW_BUCKET/input/medicare_claims.csv`

Upload both:

```bash
aws s3 cp ../glue/transform.py "s3://$RAW_BUCKET/scripts/transform.py"
aws s3 cp ../data/medicare_claims.csv "s3://$RAW_BUCKET/input/medicare_claims.csv"
```

### 3) Run the Glue job
Start the job:

```bash
aws glue start-job-run --job-name "$GLUE_JOB_NAME"
```

### 4) Verify output
The job writes Parquet to:
- `s3://$PROCESSED_BUCKET/output/medicare_claims/`

List outputs:

```bash
aws s3 ls "s3://$PROCESSED_BUCKET/output/medicare_claims/" --recursive
```

The data is also cataloged in AWS Glue as a table:
- Database: `healthcare_db`
- Table: `medicare_claims`

You can query it using Amazon Athena:

```sql
SELECT * FROM healthcare_db.medicare_claims LIMIT 10;
```

### Optional: Run from AWS Console
- Go to **AWS Glue → ETL jobs →** select the job output as `glue_job_name` → **Run**
- View logs in **CloudWatch Logs** for the job run.
