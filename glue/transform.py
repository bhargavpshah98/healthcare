import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_BUCKET", "PROCESSED_BUCKET"]
)

raw_bucket = args["RAW_BUCKET"]
processed_bucket = args["PROCESSED_BUCKET"]

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

input_path = f"s3://{raw_bucket}/input/iris.csv"
output_path = f"s3://{processed_bucket}/output/iris/"

# Read CSV with header
df = spark.read.option("header", "true").csv(input_path)

# Basic validation: drop rows with nulls in critical columns
required_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
for c in required_cols:
    df = df.filter(col(c).isNotNull())

# Example transform: cast numeric columns to double
df_clean = (
    df.withColumn("sepal_length", col("sepal_length").cast("double"))
      .withColumn("sepal_width", col("sepal_width").cast("double"))
      .withColumn("petal_length", col("petal_length").cast("double"))
      .withColumn("petal_width", col("petal_width").cast("double"))
)

# Write to Parquet (better format for analytics)
df_clean.write.mode("overwrite").parquet(output_path)

print(f"Wrote Parquet output to: {output_path}")
