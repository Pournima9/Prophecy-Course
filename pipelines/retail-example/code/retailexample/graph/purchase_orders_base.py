from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def purchase_orders_base(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("xml")\
        .option("rowTag", "purchase_item")\
        .schema(
          StructType([
            StructField("EAN13", LongType(), True), StructField("EAN5", LongType(), True), StructField("PO", LongType(), True), StructField("datetime", LongType(), True), StructField("password", StringType(), True), StructField("price", StringType(), True), StructField("product_name", StringType(), True), StructField("product_unit", StringType(), True), StructField("purchaser", StringType(), True), StructField("quantity", LongType(), True), StructField("supplier", StringType(), True)
        ])
        )\
        .load("dbfs:/databricks-datasets/retail-org/purchase_orders/purchase_orders.xml")
