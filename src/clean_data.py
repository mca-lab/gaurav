from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, year

def start_spark():
    return SparkSession.builder \
        .appName("OTTContentCleaning") \
        .master("local[*]") \
        .getOrCreate()

def clean_netflix(spark):
    df = spark.read.option("header", True).csv("data/raw/netflix_titles.csv")
    #  Add platform column
    df = df.withColumn("platform", lit("Netflix"))
    # Cast release_year to integer
    df = df.withColumn("release_year", col("release_year").cast("int"))
    # Handle date_added column, drop if unusable
    df = df.withColumn("date_added_parsed", to_date(col("date_added"), "MMMM d, yyyy"))
    # Fill missing values for key columns
    for c in ["director","cast","country","rating"]:
        df = df.fillna({c: "Unknown"})
    # Select relevant subset of columns
    df = df.select("show_id", "type", "title", "country", "release_year", "genre", "platform")
    return df

def clean_amazon(spark):
    df = spark.read.option("header", True).csv("data/raw/amazon_prime_titles.csv")
    df = df.withColumn("platform", lit("Amazon Prime"))
    # Standardise columns: rename if necessary
    df = df.withColumnRenamed("Title", "title") \
           .withColumnRenamed("Type", "type") \
           .withColumnRenamed("Country", "country") \
           .withColumnRenamed("Release Year", "release_year") \
           .withColumnRenamed("listed_in", "genre")
    df = df.withColumn("release_year", col("release_year").cast("int"))
    for c in ["director","cast","country","rating"]:
        if c in df.columns:
            df = df.fillna({c: "Unknown"})
    df = df.select("title", "type", "country", "release_year", "genre", "platform")
    return df

def main():
    spark = start_spark()
    netflix = clean_netflix(spark)
    amazon = clean_amazon(spark)
    # Align schemas: use unionByName with missing columns handled
    combined = netflix.unionByName(amazon, allowMissingColumns=True)
    # Drop duplicates and rows with missing essential info
    combined = combined.dropDuplicates(["title", "platform"])
    combined = combined.na.drop(subset=["title", "type", "platform"])
    # Save to Parquet
    combined.write.mode("overwrite").parquet("data/processed/ott_combined.parquet")
    print(" Cleaned & combined dataset saved to data/processed/ott_combined.parquet")
    spark.stop()

if __name__ == "__main__":
    main()
