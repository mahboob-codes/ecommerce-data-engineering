# Databricks notebook source

from pyspark.sql import functions as F

# Step 1: Read Silver tables
orders = spark.table("ecommerce_project.silver.order_items")
products = spark.table("ecommerce_project.silver.products")
brands = spark.table("ecommerce_project.silver.brands")
categories = spark.table("ecommerce_project.silver.category")
dates = spark.table("ecommerce_project.silver.date")
customers = spark.table("ecommerce_project.silver.customers")

# Step 2: Calculate sales amounts
orders = (
    orders
    .withColumn(
        "gross_amount",
        F.round(
            F.col("quantity") * F.col("unit_price"), 2
        )
    )
    .withColumn(
        "discount_amount",
        F.round(
            F.col("quantity")
            * F.col("unit_price")
            * F.col("discount_pct") / 100,
            2
        )
    )
)

# Step 3: Join orders with product details
gold_sales = (
    orders.alias("o")
    .join(
        products.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )

    # FIX: Product brand codes are lowercase;
    # brand table codes are uppercase.
    .join(
        brands.alias("b"),
        F.upper(F.col("p.brand_code"))
        == F.col("b.brand_code"),
        "left"
    )

    # Join category information
    .join(
        categories.alias("c"),
        F.col("p.category_code")
        == F.col("c.category_code"),
        "left"
    )

    # Join date information
   
.join(
    dates.alias("d"),
    F.col("o.dt") == F.col("d.date"),
    "left"
)


    # Keep all sales, even if customer details are missing
    .join(
        customers.alias("cu"),
        F.col("o.customer_id")
        == F.col("cu.customer_id"),
        "left"
    )

    # Step 4: Select final Gold columns
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
        F.col("o.gross_amount"),
        F.col("o.discount_amount")
    )
)

# Step 5: Write Gold sales detail table
(
    gold_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_project.gold.sales_detail")
)

print("Gold sales detail rows:", gold_sales.count())

display(gold_sales.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

gold_sales = spark.table(
    "ecommerce_project.gold.sales_detail"
)

sales_summary = (
    gold_sales
    .groupBy("order_date", "unit_price_currency", "channel")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("order_item_count"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("gross_amount"), 2).alias("gross_sales"),
        F.round(F.sum("discount_amount"), 2).alias("discount_total")
    )
    .withColumnRenamed("order_date", "dt")
)

(
    sales_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_project.gold.sales_summary")
)

print("Gold sales summary rows:", sales_summary.count())

display(sales_summary.orderBy("dt").limit(10))


# COMMAND ----------

