# Databricks notebook source
# Define the location of our uploaded files
volume_path = "/Volumes/ecommerce_project/bronze/raw_files"

# List the files in the volume
display(dbutils.fs.ls(volume_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,input_file_name

# Location of our uploaded CSV files
volume_path = "/Volumes/ecommerce_project/bronze/raw_files"

# Read products.csv
prducts_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(f"{volume_path}/products.csv")
)
display(prducts_df.limit(10))

# COMMAND ----------

prducts_df.write\
 .format("delta")\
 .mode("overwrite")\
 .saveAsTable("ecommerce_project.bronze.products")

# COMMAND ----------

spark.sql(
    """select
    * from ecommerce_project.bronze.products
    limit 10
""").show()


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,input_file_name

volume_path = "/Volumes/ecommerce_project/bronze/raw_files"

files = {
    "brands": "brands.csv",
    "category": "category.csv",
    "customers": "customers.csv",
    "date": "date.csv"
}

for table_name, file_name in files.items():
    print(f"Reading {file_name}...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{volume_path}/{file_name}")
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"ecommerce_project.bronze.{table_name}")

    print(f"SUCCESS: {table_name} table created")

# COMMAND ----------

spark.sql("SHOW TABLES IN ecommerce_project.bronze").show(truncate=False)

# COMMAND ----------

volume_path = "/Volumes/ecommerce_project/bronze/raw_files"

order_itemes_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(f"{volume_path}/order_items_*.csv")
)
order_itemes_df.printSchema()
display(order_itemes_df.limit(10))

# COMMAND ----------

order_itemes_df.write\
.format("delta")\
.mode("overwrite")\
.saveAsTable("ecommerce_project.bronze.order_items")

print("SUCCESS: order_items bronze table created")

# COMMAND ----------

order_items_count = spark.table(
    "ecommerce_project.bronze.order_items"
).count()

print(f"Total order_items records: {order_items_count:,}")

# COMMAND ----------

order_items_df = spark.table(
    "ecommerce_project.bronze.order_items"
)

order_items_df.printSchema()

display(order_items_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table("ecommerce_project.bronze.order_items")

display(
    df.select(
        "quantity",
        "unit_price_currency",
        "unit_price",
        "discount_pct",
        "tax_amount",
        "channel",
        "coupon_code"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_path = "ecommerce_project.bronze.order_items"

df = spark.table(bronze_path)

# Clean and standardize the data
silver_df = (
    df
    # Clean quantity: convert recognized number words to digits
    .withColumn(
        "quantity_clean",
        F.when(F.lower(F.trim("quantity")) == "two", F.lit("2"))
         .otherwise(F.trim("quantity"))
         .cast("int")
    )

    # Remove currency symbols and commas before converting price
    .withColumn(
        "unit_price_clean",
        F.regexp_replace(
            F.regexp_replace(F.trim("unit_price"), r"[$,]", ""),
            r"[^0-9.\-]", ""
        ).cast("decimal(18,2)")
    )

    # Remove % before converting discount to a numeric percentage
    .withColumn(
        "discount_pct_clean",
        F.regexp_replace(F.trim("discount_pct"), "%", "")
         .cast("decimal(5,2)")
    )

    # Standardize text fields
    .withColumn("unit_price_currency", F.upper(F.trim("unit_price_currency")))
    .withColumn("channel", F.lower(F.trim("channel")))
    .withColumn(
        "coupon_code",
        F.when(
            F.col("coupon_code").isNull() |
            (F.trim("coupon_code") == ""),
            None
        ).otherwise(F.upper(F.trim("coupon_code")))
    )

    # Add processing timestamp
    .withColumn("silver_processed_at", F.current_timestamp())

    # Remove the original columns we replaced
    .drop("quantity", "unit_price", "discount_pct")

    # Give the cleaned columns their final names
    .withColumnRenamed("quantity_clean", "quantity")
    .withColumnRenamed("unit_price_clean", "unit_price")
    .withColumnRenamed("discount_pct_clean", "discount_pct")
)

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.order_items")

print("Silver order_items table created successfully!")

# COMMAND ----------



# COMMAND ----------

silver_df = spark.table("ecommerce_project.silver.order_items")

silver_df.printSchema()

display(
    silver_df.select(
        "quantity",
        "unit_price_currency",
        "unit_price",
        "discount_pct",
        "tax_amount",
        "channel",
        "coupon_code"
    ).limit(20)
)

# COMMAND ----------

bronze_count = spark.table(
    "ecommerce_project.bronze.order_items"
).count()

silver_count = spark.table(
    "ecommerce_project.silver.order_items"
).count()

print(f"Bronze rows: {bronze_count:,}")
print(f"Silver rows: {silver_count:,}")
print(f"Difference: {bronze_count - silver_count:,}")

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("ecommerce_project.silver.order_items")

df.select(
    F.sum(F.when(F.col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
    F.sum(F.when(F.col("unit_price").isNull(), 1).otherwise(0)).alias("null_unit_price"),
    F.sum(F.when(F.col("discount_pct").isNull(), 1).otherwise(0)).alias("null_discount_pct"),
    F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("nonpositive_quantity"),
    F.sum(F.when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), 1).otherwise(0)).alias("invalid_discount")
).show()

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table("ecommerce_project.silver.order_items")

gold_df = (
    silver_df
    .withColumn(
        "gross_amount",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2
        )
    )
    .withColumn(
        "discount_amount",
        F.round(
            F.col("gross_amount") * F.col("discount_pct") / 100,
            2
        )
    )
    .withColumn(
        "net_amount_before_tax",
        F.round(
            F.col("gross_amount") - F.col("discount_amount"),
            2
        )
    )
    .withColumn(
        "sales_amount_including_tax",
        F.round(
            F.col("net_amount_before_tax") + F.col("tax_amount"),
            2
        )
    )
)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.gold.order_items")

print("Gold order_items table created successfully!")

# COMMAND ----------

display(
    spark.table("ecommerce_project.gold.order_items")
    .select(
        "order_id",
        "quantity",
        "unit_price_currency",
        "unit_price",
        "gross_amount",
        "discount_pct",
        "discount_amount",
        "tax_amount",
        "sales_amount_including_tax"
    )
    .limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = spark.table("ecommerce_project.gold.order_items")

# Check the currency distribution
display(
    gold_df.groupBy("unit_price_currency")
    .agg(
        F.count("*").alias("record_count"),
        F.round(F.sum("gross_amount"), 2).alias("gross_amount")
    )
    .orderBy("unit_price_currency")
)

# COMMAND ----------

print(
    "Gold rows:",
    spark.table("ecommerce_project.gold.order_items").count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     unit_price_currency,
# MAGIC     COUNT(*) AS record_count,
# MAGIC     ROUND(SUM(gross_amount), 2) AS gross_amount
# MAGIC FROM ecommerce_project.gold.order_items
# MAGIC GROUP BY unit_price_currency
# MAGIC ORDER BY unit_price_currency;

# COMMAND ----------

display(
    spark.table("ecommerce_project.gold.sales_summary")
    .orderBy("dt")
    .limit(20)
)

# COMMAND ----------

tables = [
    "customers",
    "products",
    "brands",
    "category",
    "date"
]

for table_name in tables:
    print(f"\n========== {table_name.upper()} ==========")
    df = spark.table(f"ecommerce_project.bronze.{table_name}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

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

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.customers")

print("SUCCESS: Silver customers table created")

# COMMAND ----------

spark.table("ecommerce_project.silver.customers").printSchema()

display(
    spark.table("ecommerce_project.silver.customers").limit(5)
)

# COMMAND ----------

products_df = spark.table("ecommerce_project.bronze.products")

products_df.printSchema()

display(products_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

products_df = spark.table("ecommerce_project.bronze.products")

silver_products = (
    products_df
    .withColumn("sku", F.upper(F.trim("sku")))
    .withColumn("category_code", F.lower(F.trim("category_code")))
    .withColumn("brand_code", F.lower(F.trim("brand_code")))
    .withColumn("color", F.initcap(F.trim("color")))
    .withColumn("size", F.trim("size"))
    .withColumn("material", F.initcap(F.trim("material")))

    # Remove units and convert decimal commas to decimal points
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
    .withColumn(
        "width_cm",
        F.col("width_cm").cast("decimal(10,2)")
    )
    .withColumn(
        "height_cm",
        F.col("height_cm").cast("decimal(10,2)")
    )

    # Flag invalid rating counts rather than silently deleting records
    .withColumn(
        "rating_count",
        F.when(F.col("rating_count") < 0, None)
         .otherwise(F.col("rating_count"))
    )
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.products")

print("SUCCESS: Silver products table created")

# COMMAND ----------

silver_products = spark.table("ecommerce_project.silver.products")

silver_products.printSchema()

display(
    silver_products.select(
        "product_id",
        "sku",
        "material",
        "weight_grams",
        "length_cm",
        "width_cm",
        "height_cm",
        "rating_count"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

brands_df = spark.table("ecommerce_project.bronze.brands")

brands_df.printSchema()
display(brands_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

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

print("SUCCESS: Silver Brands table created")

# COMMAND ----------

silver_brands = spark.table("ecommerce_project.silver.brands")

silver_brands.printSchema()

display(
    silver_brands.select(
        "brand_code",
        "brand_name",
        "category_code"
    ).limit(10)
)

# COMMAND ----------

category_df = spark.table("ecommerce_project.bronze.category")

category_df.printSchema()
display(category_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

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

print("SUCCESS: Silver Category table created")

# COMMAND ----------

silver_category = spark.table("ecommerce_project.silver.category")

silver_category.printSchema()
display(silver_category.orderBy("category_code"))

# COMMAND ----------

date_df = spark.table("ecommerce_project.bronze.date")

date_df.printSchema()
display(date_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

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

print("SUCCESS: Silver Date table created")

# COMMAND ----------

silver_date = spark.table("ecommerce_project.silver.date")

silver_date.printSchema()

display(
    silver_date
    .orderBy("date")
    .limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    "order_items",
    "customers",
    "products",
    "brands",
    "category",
    "date"
]

for table_name in tables:
    df = spark.table(f"ecommerce_project.silver.{table_name}")
    print(f"{table_name}: {df.count()} rows")

# COMMAND ----------

from pyspark.sql import functions as F

checks = {
    "customers": "customer_id",
    "products": "product_id",
    "brands": "brand_code",
    "category": "category_code",
    "date": "date"
}

for table_name, key in checks.items():
    df = spark.table(f"ecommerce_project.silver.{table_name}")

    duplicates = (
        df.groupBy(key)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    null_keys = df.filter(F.col(key).isNull()).count()

    print(
        f"{table_name}: duplicate keys = {duplicates}, "
        f"null keys = {null_keys}"
    )

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.table("ecommerce_project.silver.customers")

null_customers = customers.filter(
    F.col("customer_id").isNull()
)

print("Rows with null customer_id:", null_customers.count())
display(null_customers.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.table("ecommerce_project.silver.customers")

customers.filter(
    F.col("customer_id").isNull()
).agg(
    F.count("*").alias("total_rows"),
    F.count("phone").alias("rows_with_phone"),
    F.count("country").alias("rows_with_country"),
    F.count("state").alias("rows_with_state")
).show()

# COMMAND ----------

duplicate_ids = (
    customers
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_ids)

# COMMAND ----------

display(
    customers.join(
        duplicate_ids.select("customer_id"),
        on="customer_id",
        how="inner"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.table("ecommerce_project.silver.customers")

print("Total rows:", customers.count())
print("Null customer IDs:", customers.filter(
    F.col("customer_id").isNull()
).count())

print("Duplicate non-null IDs:",
    customers.filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

customers = spark.table("ecommerce_project.silver.customers")

valid_customers = customers.filter(
    F.col("customer_id").isNotNull()
)

valid_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.silver.customers_valid")

print("Valid customer rows:", valid_customers.count())

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("ecommerce_project.silver.order_items")
customers = spark.table("ecommerce_project.silver.customers_valid")

# Find order items that match a valid customer
matched_orders = orders.join(
    customers.select("customer_id"),
    on="customer_id",
    how="inner"
)

# Find order items without a matching customer
unmatched_orders = orders.join(
    customers.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

print("Total order items:", orders.count())
print("Matched order items:", matched_orders.count())
print("Unmatched order items:", unmatched_orders.count())

# COMMAND ----------

from pyspark.sql import functions as F

display(
    unmatched_orders
    .groupBy("customer_id")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(unmatched_orders.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

bronze_customers = spark.table(
    "ecommerce_project.bronze.customers"
)

missing_customer_ids = (
    unmatched_orders
    .select("customer_id")
    .distinct()
)

bronze_check = missing_customer_ids.join(
    bronze_customers
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
        .distinct(),
    on="customer_id",
    how="left_semi"
)

print(
    "Unmatched customer IDs found in Bronze:",
    bronze_check.count()
)

display(bronze_check.limit(10))

# COMMAND ----------


from pyspark.sql import functions as F

orders = spark.table("ecommerce_project.silver.order_items").alias("o")
products = spark.table("ecommerce_project.silver.products").alias("p")
brands = spark.table("ecommerce_project.silver.brands").alias("b")
categories = spark.table("ecommerce_project.silver.category").alias("c")
dates = spark.table("ecommerce_project.silver.date").alias("d")
customers = spark.table("ecommerce_project.silver.customers_valid").alias("cu")

gold_sales = (
    orders
    .join(products, F.col("o.product_id") == F.col("p.product_id"), "left")
    .join(brands, F.col("p.brand_code") == F.col("b.brand_code"), "left")
    .join(categories, F.col("p.category_code") == F.col("c.category_code"), "left")
    .join(dates, F.col("o.dt") == F.col("d.date"), "left")
    .join(customers, F.col("o.customer_id") == F.col("cu.customer_id"), "left")
    .select(
        F.col("o.dt").alias("order_date"),
        F.col("o.order_id"),
        F.col("o.item_seq"),
        F.col("o.customer_id"),
        F.col("cu.country"),
        F.col("cu.state"),
        F.col("o.product_id"),
        F.col("p.sku"),
        F.col("p.category_code"),
        F.col("c.category_name"),
        F.col("p.brand_code"),
        F.col("b.brand_name"),
        F.col("p.color"),
        F.col("o.unit_price_currency"),
        F.col("o.channel"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.discount_pct"),
        F.col("o.tax_amount"),
        F.col("d.year"),
        F.col("d.quarter"),
        F.month(F.col("o.dt")).alias("month"),
        F.col("o.coupon_code"),
        F.round(
            F.col("o.quantity") * F.col("o.unit_price"), 2
        ).alias("gross_amount"),
        F.round(
            F.col("o.quantity") * F.col("o.unit_price")
            * F.col("o.discount_pct") / 100, 2
        ).alias("discount_amount")
    )
)

gold_sales.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_project.gold.sales_detail")


# COMMAND ----------

gold_sales = spark.table("ecommerce_project.gold.sales_detail")

print("Gold sales rows:", gold_sales.count())
display(gold_sales.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

products = spark.table("ecommerce_project.silver.products")
brands = spark.table("ecommerce_project.silver.brands")

missing_brands = (
    products.select("brand_code").distinct()
    .join(
        brands.select("brand_code").distinct(),
        on="brand_code",
        how="left_anti"
    )
)

print("Product brand codes missing from Brands:",
      missing_brands.count())

display(missing_brands)

# COMMAND ----------

