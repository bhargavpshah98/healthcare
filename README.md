# Healthcare ETL Pipeline (AWS + Terraform)

## Overview
This project implements a comprehensive ETL pipeline using AWS services and Terraform.

The pipeline:
- Extracts a public CSV dataset
- Stores it in Amazon S3 (raw layer)
- Transforms the data using AWS Glue (PySpark) with data validation, cleaning, and aggregations
- Writes optimized Parquet output to S3 (processed layer) in detailed and aggregated formats
- Catalogs the data in AWS Glue for querying via Athena
- Runs nightly via scheduled trigger for automated processing

## Security and Permissions

The infrastructure is configured with least-privilege IAM permissions and secure S3 bucket settings:

### S3 Bucket Permissions
- **Private Access**: All buckets have public access blocked (no public ACLs, policies, or bucket-level access)
- **Encryption**: Default AES256 encryption for all objects
- **Access Control**: Access restricted to authorized IAM roles only

### Glue Job Permissions
- **IAM Role**: Dedicated role (`healthcare-glue-role`) with assume role policy for Glue service
- **Service Permissions**: `AWSGlueServiceRole` policy provides:
  - CloudWatch Logs access for monitoring
  - Glue catalog and job execution permissions
  - Basic AWS service access
- **S3 Access Policy**: Custom policy with least privilege:
  - List buckets (for validation)
  - Read objects from raw bucket
  - Full access (read/write/delete) to processed bucket
- **No Over-Provisioning**: Permissions scoped to specific buckets, not account-wide access

### Data Validation
- **Column Existence Check**: Validates that all required columns are present in the input CSV
- **Null Value Filtering**: Removes rows with null values in critical columns (npi, provider_type, amounts)
- **Type Casting**: Ensures numeric columns are properly cast to appropriate types (double, bigint)
- **HTTP Error Detection**: Checks input file for HTTP error responses (e.g., 404) instead of valid CSV data

### Error Handling
- **Missing Columns**: Raises `ValueError` with detailed message if required columns are missing
- **Invalid Data Format**: Fails gracefully with descriptive error messages for malformed input
- **Execution Failures**: Leverages Glue's built-in retry mechanisms and failure notifications

### Logging
- **CloudWatch Integration**: All job logs are sent to AWS CloudWatch Logs for monitoring
- **Continuous Logging**: Enabled for real-time log streaming during job execution
- **Execution Metrics**: Tracks job performance, errors, and completion status
- **Output Confirmation**: Logs successful write operations with S3 paths

### Monitoring
- View logs in AWS CloudWatch Logs under `/aws-glue/jobs/`
- Set up CloudWatch alarms for job failures
- Use Glue job metrics for performance monitoring

## Testing
The project includes unit and integration tests to ensure reliability.

### Prerequisites
- Python 3.8+
- Install test dependencies: `pip install -r requirements-test.txt`

### Run Tests
From the repo root:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=glue tests/

# Run specific test file
pytest tests/test_transform.py
```

### Test Coverage
- **Unit Tests**: Test utility functions like column normalization
- **Data Quality Tests**: Validate data transformations, type casting, and aggregations using PySpark
- **Integration Tests**: Ensure end-to-end logic works correctly

Tests use sample data and mock Spark sessions where possible to avoid requiring full AWS environment.

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
- `s3://$PROCESSED_BUCKET/output/detailed/` (full detailed data)
- `s3://$PROCESSED_BUCKET/output/aggregated/` (aggregated by provider)

List outputs:

```bash
aws s3 ls "s3://$PROCESSED_BUCKET/output/detailed/" --recursive
aws s3 ls "s3://$PROCESSED_BUCKET/output/aggregated/" --recursive
```

The detailed data is also cataloged in AWS Glue as a table:
- Database: `healthcare_db`
- Table: `medicare_claims`

You can query it using Amazon Athena:

```sql
SELECT * FROM healthcare_db.medicare_claims LIMIT 10;
```

The job is scheduled to run nightly at 2 AM UTC via AWS Glue Trigger.

### Optional: Run from AWS Console
- Go to **AWS Glue → ETL jobs →** select the job output as `glue_job_name` → **Run**
- View logs in **CloudWatch Logs** for the job run.
