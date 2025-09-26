from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def products_base_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("product_id", StringType(), True), StructField("product_category", StringType(), True), StructField("product_name", StringType(), True), StructField("sales_price", DoubleType(), True), StructField("EAN13", LongType(), True), StructField("EAN5", IntegerType(), True), StructField("product_unit", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ";")\
        .csv("dbfs:/databricks-datasets/retail-org/products/products.csv")
