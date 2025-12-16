from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import col, count, avg, max, min, first, dense_rank, when, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

def write_to_postgres(
    df_to_store,
    table_name: str,
    mode: str = "overwrite",
    jdbc_url: str = "jdbc:postgresql://localhost:5432/airflow",
    user: str = "airflow",
    password: str = "airflow"
):
    props = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df_to_store.write.mode(mode).jdbc( url=jdbc_url, table=table_name, properties=props )
        print(f"Data successfully written to table: {table_name}")

    except AnalysisException as e:
        print(f"ANALYSIS EXCEPTION while writing {table_name}")
        raise e

    except Py4JJavaError as e:
        print(f"JAVA EXCEPTION while writing {table_name}")
        print(e.java_exception)
        raise e

    except Exception as e:
        print(f"UNKNOWN ERROR while writing {table_name}")
        raise e


def write_parquet(gold_data_dir, gold_df, gold_df_name):
    gold_data_path = gold_data_dir / gold_df_name
    gold_df.write.mode("overwrite").parquet(str(gold_data_path))


def create_product_summary(clenaed_df):
    products_gold = (
        clenaed_df
        .groupBy("product_id")
        .agg(
            first("product_title", ignorenulls=True).alias("product_title"),
            first("category", ignorenulls=True).alias("category"),
            spark_round(avg("rating"), 2).alias("avg_rating"),
            max("rating_count").alias("rating_count"),
            min("discounted_price").alias("discounted_price"),
            first("actual_price", ignorenulls=True).alias("actual_price"),
            first("discount_percentage", ignorenulls=True).alias("discount_percentage"),
            first("price_difference", ignorenulls=True).alias("price_difference"),
            first("rating_bucket", ignorenulls=True).alias("rating_bucket"),
            first("rating_strength", ignorenulls=True).alias("rating_strength")
        )   
    )
    return products_gold


def create_category_summary(cleaned_df):
    category_gold_df = (
        cleaned_df
        .groupBy("category")
        .agg(
            count("product_id").alias("total_products"),
            spark_round(avg("rating"), 2).alias("avg_rating"),
            spark_round(avg("rating_count"), 0).alias("avg_rating_count"),
            spark_round(avg("discount_percentage"), 2).alias("avg_discount_percentage"),
            spark_round(avg("price_difference"), 2).alias("avg_price_difference")
        )
    )
    return category_gold_df


def create_top_products(cleaned_df):
    rating_strength_score = when(col("rating_count") >= 10000, 4) \
        .when(col("rating_count") >= 1000, 3) \
        .when(col("rating_count") >= 100, 2) \
        .otherwise(1)

    rating_bucket_score = when(col("rating") >= 4.5, 4) \
        .when(col("rating") >= 4.0, 3) \
        .when(col("rating") >= 3.0, 2) \
        .otherwise(1)

    window_spec = Window.partitionBy("category").orderBy(
        col("rating_count").desc(),
        col("rating_strength_score").desc(),
        col("rating_bucket_score").desc()
    )

    top_products_gold_df = (
        cleaned_df
        .withColumn("rating_strength_score", rating_strength_score)
        .withColumn("rating_bucket_score", rating_bucket_score)
        .withColumn("rank", dense_rank().over(window_spec))
        .filter(col("rank") <= 5)
        .select(
            "product_id",
            "product_title",
            "category",
            "rating",
            "rating_count",
            "discounted_price",
            "rank"
        )
    )

    return top_products_gold_df


def create_discount_effectiveness(cleaned_df):
    discount_effectiveness_gold_df = cleaned_df.groupBy("discount_bucket") \
        .agg(
            spark_round(avg(col("rating")), 2).alias("avg_rating"),
            spark_round(avg(col("rating_count")), 2).alias("avg_rating_count"),
            count(col("product_id")).alias("total_products")
        )
        
    return discount_effectiveness_gold_df


def main():
    spark = SparkSession.builder.appName("AmazonGold").config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    ).getOrCreate()

    print("Spark started:", spark.version)

    # --------- project root and gold data path ---------
    project_root = Path.cwd()
    gold_data_dir = project_root / "data" / "gold"
    gold_data_dir.mkdir(parents=True, exist_ok=True)
    
    # -------- read clenaed data ----------------
    cleaned_data_path = project_root / "data" / "processed" / "amazon_sales_cleaned"
    # cleaned_data_path = Path("/data/processed/amazon_sales_cleaned.csv")
    # cleaned_df = spark.read.parquet(str(cleaned_data_path))
    cleaned_df = spark.read.parquet(str(cleaned_data_path))
    cleaned_df.cache() # 
    
    # --------------- create porduct summary gold df, write and store into db --------------
    product_summary_gold_df = create_product_summary(cleaned_df)
    write_parquet(gold_data_dir, product_summary_gold_df, "gold_product_summary")
    write_to_postgres(product_summary_gold_df, "product_summary")
    print("Product summary: ", product_summary_gold_df.count())
    print("Cleaned df: ", cleaned_df.count())
    
    # --------------- create category summary gold df, write and store into db --------------
    category_summary_gold_df = create_category_summary(cleaned_df)
    write_parquet(gold_data_dir, category_summary_gold_df, "gold_category_summary")
    write_to_postgres(category_summary_gold_df, "category_summary")
    print("Category summary: ", category_summary_gold_df.count())
    print("Cleaned df: ", cleaned_df.count())
    
    # --------- create top products summary gold df, write and store into db --------------
    top_products_summary_gold_df = create_top_products(cleaned_df)
    write_parquet(gold_data_dir, top_products_summary_gold_df, "gold_top_products_summary")
    write_to_postgres(top_products_summary_gold_df, "top_products")
    print("Top products summary: ", top_products_summary_gold_df.count())
    print("Cleaned df: ", cleaned_df.count())
    
    # --------  create discount effectiveness gold df, write and store into db  ---------
    discount_effectiveness_gold_df = create_discount_effectiveness(cleaned_df)
    write_parquet(gold_data_dir, discount_effectiveness_gold_df, "gold_discount_effectiveness")
    write_to_postgres(discount_effectiveness_gold_df, "discount_effectiveness")
    print("Discount Effectiveness summary: ", discount_effectiveness_gold_df.count())
    print("Cleaned df: ", cleaned_df.count())
    
    spark.stop()

if __name__ == "__main__":
    main()
    