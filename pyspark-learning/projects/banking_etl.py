"""
Banking ETL Pipeline

This script demonstrates a simple end-to-end PySpark ETL flow for banking transactions.
It reads transaction data from the raw CSV location, cleans it, aggregates the totals
per customer, and writes the result as Parquet to the gold data folder.
"""

from pathlib import Path
import sys

import pyspark.sql.functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from utils.spark_session import create_spark_session


def run_banking_etl():
    """Run the banking ETL process end to end."""
    spark = create_spark_session()

    try:
        input_path = PROJECT_ROOT / "data" / "raw" / "transactions.csv"
        output_path = PROJECT_ROOT / "data" / "gold" / "customer_transaction_totals"

        print("Step 1: Reading CSV with inferred schema...")
        raw_df = (
            spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(str(input_path))
        )
        raw_df.printSchema()
        raw_df.show(truncate=False)

        print("Step 2: Removing duplicate rows...")
        deduped_df = raw_df.dropDuplicates()
        print(f"Rows after deduplication: {deduped_df.count()}")

        print("Step 3: Handling null values...")
        cleaned_df = (
            deduped_df
            .withColumn(
                "customer_id",
                F.when(F.col("customer_id").isNull(), F.lit("UNKNOWN")).otherwise(F.col("customer_id")),
            )
            .withColumn(
                "txn_type",
                F.when(F.col("txn_type").isNull(), F.lit("UNKNOWN")).otherwise(F.col("txn_type")),
            )
            .withColumn(
                "amount",
                F.when(F.col("amount").isNull(), F.lit(0.0)).otherwise(F.col("amount")),
            )
        )
        cleaned_df.show(truncate=False)

        print("Step 4: Aggregating total transaction amount per customer...")
        aggregated_df = (
            cleaned_df.groupBy("customer_id")
            .agg(F.sum("amount").alias("total_amount"))
            .orderBy("customer_id")
        )
        aggregated_df.show(truncate=False)

        print("Step 5: Writing parquet output to gold layer...")
        output_path.mkdir(parents=True, exist_ok=True)
        aggregated_df.write.mode("overwrite").parquet(str(output_path))
        print(f"Parquet written to: {output_path}")

        return aggregated_df

    finally:
        spark.stop()


if __name__ == "__main__":
    run_banking_etl()
