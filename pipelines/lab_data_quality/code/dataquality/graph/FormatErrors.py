from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def FormatErrors(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        explode(
            array(
              struct(
                (col("distinct_sales_id") != col("sales_id_count")).alias("is_error"), 
                lit("sales_id not distinct").alias("error_message")
              ), 
              struct(
                (col("sales_id_null_count") != lit(0)).alias("is_error"), 
                lit("there are null sales_ids").alias("error_message")
              ), 
              struct(
                (col("customer_id_null_count") != lit(0)).alias("is_error"), 
                lit("there are null customer_ids").alias("error_message")
              ), 
              struct(
                (col("product_id_null_count") != lit(0)).alias("is_error"), 
                lit("there are null product_ids").alias("error_message")
              )
            )
          )\
          .alias("errors")
    )
