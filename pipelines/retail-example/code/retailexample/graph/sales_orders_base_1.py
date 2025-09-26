from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def sales_orders_base_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .schema(
          StructType([
            StructField("clicked_items", ArrayType(ArrayType(StringType(), True), True), True), StructField("customer_id", StringType(), True), StructField("customer_name", StringType(), True), StructField("number_of_line_items", StringType(), True), StructField("order_datetime", StringType(), True), StructField("order_number", LongType(), True), StructField("ordered_products", ArrayType(
            StructType([
              StructField("curr", StringType(), True), StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("price", LongType(), True), StructField("promotion_info", StructType([
                StructField("promo_disc", DoubleType(), True), StructField("promo_id", LongType(), True), StructField("promo_item", StringType(), True), StructField("promo_qty", LongType(), True)
              ]), True), StructField("qty", LongType(), True), StructField("unit", StringType(), True)
          ]), 
            True
          ), True), StructField("promo_info", ArrayType(
            StructType([
              StructField("promo_disc", DoubleType(), True), StructField("promo_id", LongType(), True), StructField("promo_item", StringType(), True), StructField("promo_qty", LongType(), True)
          ]), 
            True
          ), True)
        ])
        )\
        .load("dbfs:/databricks-datasets/retail-org/sales_orders/")
