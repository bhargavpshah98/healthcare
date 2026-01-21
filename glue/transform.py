import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.functions import lower, trim, regexp_replace

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_BUCKET", "PROCESSED_BUCKET"]
)

raw_bucket = args["RAW_BUCKET"]
processed_bucket = args["PROCESSED_BUCKET"]

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

input_path = f"s3://{raw_bucket}/input/medicare_claims.csv"
output_path = f"s3://{processed_bucket}/output/medicare_claims/"

# Defensive check: confirm the file *looks like* CSV (not an HTTP error payload saved as a file)
sample_lines = sc.textFile(input_path).take(5)
sample_blob = "\n".join(sample_lines).lower()
likely_http_error = any(
    marker in sample_blob
    for marker in [
        '"status":404',
        "'status':404",
        "404",
        "not found",
        "data.cms.gov",
        '"error"',
        '"message"',
    ]
)

if len(sample_lines) == 0 or likely_http_error:
    raise ValueError(
        "Input file does not look like the expected Medicare claims CSV. "
        "It appears to contain an HTTP error payload (e.g., 404) rather than tabular data.\n"
        f"Checked path: {input_path}\n"
        "First few lines:\n"
        + "\n".join(sample_lines)
    )

# Read CSV with header
df = spark.read.option("header", "true").csv(input_path)

def normalize_col_name(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
    )

# Normalize column names to make the job resilient to minor header formatting differences
normalized_cols = [normalize_col_name(c) for c in df.columns]
df = df.toDF(*normalized_cols)

# Rename columns to match expected schema
df = df.withColumnRenamed("rndrng_npi", "npi") \
       .withColumnRenamed("rndrng_prvdr_type", "provider_type") \
       .withColumnRenamed("avg_sbmtd_chrg", "total_submitted_charge_amount") \
       .withColumnRenamed("avg_mdcr_pymt_amt", "total_medicare_payment_amount")

required_cols = {
    "npi",
    "provider_type",
    "total_submitted_charge_amount",
    "total_medicare_payment_amount",
}

missing = sorted(required_cols - set(df.columns))
if missing:
    raise ValueError(
        "Input CSV is missing required columns for this transform.\n"
        f"Missing columns: {missing}\n"
        f"Found columns: {sorted(df.columns)}\n"
        f"Checked path: {input_path}\n"
        "Tip: confirm you uploaded the *actual* CSV (not a 404/JSON error page) to this S3 key."
    )

for c in required_cols:
    df = df.filter(col(c).isNotNull())


# Example transform: cast numeric columns to double
df_clean = (
    df.withColumn(
        "total_submitted_charge_amount",
        col("total_submitted_charge_amount").cast("double")
    )
    .withColumn(
        "total_medicare_payment_amount",
        col("total_medicare_payment_amount").cast("double")
    )
)


# Write to Parquet (better format for analytics)
df_clean.write.mode("overwrite").parquet(output_path)

print(f"Wrote Parquet output to: {output_path}")
