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
Public Iris dataset (CSV):
https://github.com/mwaskom/seaborn-data/blob/master/iris.csv

## Deployment Steps

### 1. Configure AWS credentials
```bash
aws configure
