import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestDataQuality:
    @pytest.fixture(scope="session")
    def spark(self):
        return SparkSession.builder \
            .appName("test") \
            .master("local[*]") \
            .getOrCreate()

    def test_column_types_after_casting(self, spark):
        # Create sample data
        data = [
            ("1234567890", "Internal Medicine", "100.50", "80.25", 10, 5.0, 2.0),
            ("0987654321", "Family Practice", "200.75", "150.50", 20, 10.0, 4.0)
        ]
        columns = ["npi", "provider_type", "total_submitted_charge_amount",
                  "total_medicare_payment_amount", "tot_benes", "tot_srvcs", "tot_bene_day_srvcs"]

        df = spark.createDataFrame(data, columns)

        # Apply casting like in transform.py
        df_cast = df \
            .withColumn("total_submitted_charge_amount", col("total_submitted_charge_amount").cast("double")) \
            .withColumn("total_medicare_payment_amount", col("total_medicare_payment_amount").cast("double")) \
            .withColumn("tot_benes", col("tot_benes").cast("bigint")) \
            .withColumn("tot_srvcs", col("tot_srvcs").cast("double")) \
            .withColumn("tot_bene_day_srvcs", col("tot_bene_day_srvcs").cast("double"))

        # Check types
        assert df_cast.schema["total_submitted_charge_amount"].dataType.typeName() == "double"
        assert df_cast.schema["tot_benes"].dataType.typeName() == "long"

    def test_null_filtering(self, spark):
        data = [
            ("123", "Type1", "100.0", "80.0"),
            ("456", None, "200.0", "150.0"),  # Null provider_type
            ("789", "Type2", None, "120.0")   # Null charge
        ]
        columns = ["npi", "provider_type", "total_submitted_charge_amount", "total_medicare_payment_amount"]

        df = spark.createDataFrame(data, columns)

        # Apply null filtering like in transform.py
        required_cols = ["npi", "provider_type", "total_submitted_charge_amount", "total_medicare_payment_amount"]
        for c in required_cols:
            df_filtered = df.filter(col(c).isNotNull())

        # Should have 1 row left (first one)
        assert df_filtered.count() == 1

    def test_aggregation_output(self, spark):
        # Create sample data for aggregation testing
        data = [
            ("123", "Type1", 10, 5.0, 100.0, 80.0),
            ("123", "Type1", 15, 7.0, 150.0, 120.0),  # Same NPI
            ("456", "Type2", 20, 10.0, 200.0, 160.0)
        ]
        columns = ["npi", "provider_type", "tot_benes", "tot_srvcs", "total_submitted_charge_amount", "total_medicare_payment_amount"]

        df = spark.createDataFrame(data, columns)

        # Apply aggregation like in transform.py
        df_agg = df.groupBy("npi", "provider_type").agg(
            {"tot_benes": "sum", "tot_srvcs": "sum", "total_submitted_charge_amount": "sum", "total_medicare_payment_amount": "sum"}
        )

        # Check results
        results = df_agg.collect()
        assert len(results) == 2  # Two unique NPIs

        # Find the aggregated row for NPI 123
        row_123 = next(row for row in results if row["npi"] == "123")
        assert row_123["sum(tot_benes)"] == 25  # 10 + 15
        assert row_123["sum(total_submitted_charge_amount)"] == 250.0  # 100 + 150