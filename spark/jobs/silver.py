from pyspark.sql import SparkSession
from pyspark.sql.functions import when, regexp_replace, col
from pathlib import Path


def main():
    spark = SparkSession.builder.appName("Amazon_silver").getOrCreate()
    print("Spark started:", spark.version)

    raw_data_path = Path("/data/raw/amazon_sales_raw.csv")
    processed_data_dir = Path("/data/processed")
    processed_data_dir.mkdir(parents=True, exist_ok=True)
    processed_data_path = processed_data_dir / "amazon_sales_cleaned"

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(str(raw_data_path))
    )

    def remove_symbol_and_cast(column_name, target_type):
        cleaned_symbol = regexp_replace(col(column_name), "[^\\d.]", "")
        return when(
            cleaned_symbol.rlike(r"^\\d+(\\.\\d+)?$"),
            cleaned_symbol.cast(target_type)
        ).otherwise(None)

    df = (
        df
        .withColumn("discounted_price", remove_symbol_and_cast("discounted_price", "float"))
        .withColumn("actual_price", remove_symbol_and_cast("actual_price", "float"))
        .withColumn("discount_percentage", remove_symbol_and_cast("discount_percentage", "int"))
        .withColumn("rating_count", remove_symbol_and_cast("rating_count", "int"))
        .withColumn("rating", col("rating").cast("float"))
        .dropna()
        .withColumn("price_difference", col("actual_price") - col("discounted_price"))
    )

    df = df.withColumn(
        "rating_bucket",
        when(col("rating") >= 4.5, "Excellent")
        .when(col("rating") >= 4.0, "Good")
        .when(col("rating") >= 3.0, "Average")
        .otherwise("Poor")
    )

    df = df.withColumn(
        "review_strength",
        when(col("rating_count") >= 10000, "Very High")
        .when(col("rating_count") >= 1000, "High")
        .when(col("rating_count") >= 100, "Medium")
        .otherwise("Low")
    )

    df = df.withColumn(
        "discount_bucket",
        when(col("discount_percentage") >= 80, "Very High Discount")
        .when(col("discount_percentage") >= 50, "High Discount")
        .when(col("discount_percentage") >= 20, "Medium Discount")
        .otherwise("Low Discount")
    )

    df.write.mode("overwrite").parquet(str(processed_data_path))
    spark.stop()


if __name__ == "__main__":
    main()
