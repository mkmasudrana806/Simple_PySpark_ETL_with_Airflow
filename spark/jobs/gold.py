from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import col, avg, max, min, first, round as spark_round


def create_product_summary(df):
    products_gold = (
    df
    .groupBy("product_id")
    .agg(
        first("product_name", ignorenulls=True).alias("product_name"),
        first("category", ignorenulls=True).alias("category"),
        spark_round(avg("rating"), 2).alias("avg_rating"),
        max("rating_count").alias("rating_count"),
        min("discounted_price").alias("discounted_price"),
        first("actual_price", ignorenulls=True).alias("actual_price"),
        first("discount_percentage", ignorenulls=True).alias("discount_percentage"),
        first("price_difference", ignorenulls=True).alias("price_difference"),
        first("rating_bucket", ignorenulls=True).alias("rating_bucket"),
        first("review_strength", ignorenulls=True).alias("rating_strength")
        )   
    )
    
    return products_gold

# def create_category_summary(df):
#     return ...

# def create_top_products(df):
#     return ...

# def create_discount_effectiveness(df):
#     return ...

def write_parquet(gold_data_dir, gold_df, gold_df_name):
    gold_data_path = gold_data_dir / gold_df_name
    gold_df.write.mode("overwrite").parquet(str(gold_data_path))
    
    
def main():
    spark = SparkSession.builder.appName("Amazon_silver").getOrCreate()
    print("Spark started:", spark.version)
    
    # --------- project root and gold data path ---------
    project_root = Path.cwd()
    gold_data_dir = project_root / "data" / "gold"
    gold_data_dir.mkdir(parents=True, exist_ok=True)
    
    # -------- read clenaed data ----------------
    cleaned_data_path = project_root / "data" / "processed" / "amazon_sales_cleaned"
    # cleaned_data_path = Path("/data/processed/amazon_sales_cleaned.csv")
    cleaned_df = spark.read.parquet(str(cleaned_data_path))

    # --------------- create porduct gold df, write and store into db --------------
    product_summary_gold_df = create_product_summary(cleaned_df)
    write_parquet(gold_data_dir, product_summary_gold_df, "gold_product_summary")
    
    # category_summary_gold_df = create_category_summary(cleaned_df)
    # top_product_gold_df = create_top_products(cleaned_df)
    # discount_effectiveness_gold_df = create_discount_effectiveness(cleaned_df)

    # write_to_postgres(product_df, "product_summary")
    # write_to_postgres(category_df, "category_summary")
    # write_to_postgres(top_df, "top_products")
    # write_to_postgres(discount_df, "discount_effectiveness")

    spark.stop()


if __name__ == "__main__":
    main()
    