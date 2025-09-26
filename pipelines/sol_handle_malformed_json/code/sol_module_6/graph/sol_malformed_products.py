from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *

def sol_malformed_products(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .option("mode", "DROPMALFORMED")\
        .schema(
          StructType([
            StructField("date_introduced", StringType(), True), StructField("extra_field", StringType(), True), StructField("main_category", StringType(), True), StructField("name", StringType(), True), StructField("price_usd_cents", StringType(), True), StructField("product_id", LongType(), True), StructField("sub_category", StringType(), True)
        ])
        )\
        .load("dbfs:/course_lab/rainforest_biz_sources/product_line_b/product_line_b.json")
