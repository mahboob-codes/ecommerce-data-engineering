# Databricks notebook source

from pyspark.sql import functions as F

print("Starting Silver transformations...")


# COMMAND ----------


# 1. Clean Order Items
df = spark.table("ecommerce_project.bronze.order_items")

silver_order_items = (
    df
    .withColumn(
        "quantity",
        F.when(
            F.lower(F.trim("quantity")) == "two", F.lit("2")
        ).otherwise(F.trim("quantity")).cast("int")
    )
    .withColumn(
        "unit_price",
        F.regexp_replace(
            F.regexp_replace(F.trim("unit_price"), r"[$,]", ""),
            r"[^0-9.\-]", ""
        ).cast("decimal(18,2)")
    )
    .withColumn(
        "discount_pct",
        F.regexp_replace(F.trim("discount_pct"), "%", "")
        .cast("decimal(5,2)")
    )
    .withColumn(
        "unit_price_currency",
        F.upper(F.trim("unit_price_currency"))
    )
    .withColumn("channel", F.lower(F.trim("channel")))
    .withColumn(
        "coupon_code",
        F.when(
            F.col("coupon_code").isNull() |
            (F.trim("coupon_code") == ""),
            None
        ).otherwise(F.upper(F.trim("coupon_code")))
    )
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_order_items.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.order_items")

print("Silver order_items created:", silver_order_items.count())


# COMMAND ----------


# 2. Clean Customers
customers_df = spark.table("ecommerce_project.bronze.customers")

silver_customers = (
    customers_df
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn(
        "phone",
        F.col("phone").cast("decimal(20,0)").cast("string")
    )
    .withColumn("country_code", F.upper(F.trim("country_code")))
    .withColumn("country", F.initcap(F.trim("country")))
    .withColumn("state", F.upper(F.trim("state")))
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.customers")

print("Silver customers created:", silver_customers.count())


# 3. Clean Products
products_df = spark.table("ecommerce_project.bronze.products")

silver_products = (
    products_df
    .withColumn("sku", F.upper(F.trim("sku")))
    .withColumn("category_code", F.lower(F.trim("category_code")))
    .withColumn("brand_code", F.lower(F.trim("brand_code")))
    .withColumn("color", F.initcap(F.trim("color")))
    .withColumn("size", F.trim("size"))
    .withColumn("material", F.initcap(F.trim("material")))
    .withColumn(
        "weight_grams",
        F.regexp_replace(F.trim("weight_grams"), r"[^0-9.\-]", "")
        .cast("decimal(10,2)")
    )
    .withColumn(
        "length_cm",
        F.regexp_replace(F.trim("length_cm"), ",", ".")
        .cast("decimal(10,2)")
    )
    .withColumn("width_cm", F.col("width_cm").cast("decimal(10,2)"))
    .withColumn("height_cm", F.col("height_cm").cast("decimal(10,2)"))
    .withColumn(
        "rating_count",
        F.when(F.col("rating_count") < 0, None)
        .otherwise(F.col("rating_count"))
    )
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.products")

print("Silver products created:", silver_products.count())


# COMMAND ----------

from pyspark.sql import functions as F

# 4. Clean Brands
brands_df = spark.table("ecommerce_project.bronze.brands")

silver_brands = (
    brands_df
    .withColumn("brand_code", F.upper(F.trim("brand_code")))
    .withColumn("brand_name", F.trim("brand_name"))
    .withColumn("category_code", F.lower(F.trim("category_code")))
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_brands.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.brands")

print("Silver brands created:", silver_brands.count())


# 5. Clean Category
category_df = spark.table("ecommerce_project.bronze.category")

silver_category = (
    category_df
    .withColumn("category_code", F.lower(F.trim("category_code")))
    .withColumn("category_name", F.trim("category_name"))
    .withColumn("silver_processed_at", F.current_timestamp())
    .dropDuplicates(["category_code"])
)

silver_category.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.category")

print("Silver category created:", silver_category.count())


# 6. Clean Date
date_df = spark.table("ecommerce_project.bronze.date")

silver_date = (
    date_df
    .withColumn("day_name", F.date_format(F.col("date"), "EEEE"))
    .withColumn("quarter", F.quarter(F.col("date")))
    .withColumn("week_of_year", F.weekofyear(F.col("date")))
    .withColumn("silver_processed_at", F.current_timestamp())
    .dropDuplicates(["date"])
)

silver_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.date")

print("Silver date created:", silver_date.count())
