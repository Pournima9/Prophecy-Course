from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        col("signup_date"), 
        coalesce(col("valid_from"), col("signup_date")).alias("valid_from"), 
        expr("if((valid_ahead IS NULL), NULL, valid_to)").alias("valid_to"), 
        col("valid_from").isNull().alias("is_original"), 
        col("valid_ahead").isNull().alias("is_current"), 
        col("customer_name"), 
        col("phone_number"), 
        col("street_number"), 
        col("street_name"), 
        col("unit"), 
        col("city"), 
        col("state"), 
        col("postcode"), 
        col("LAT"), 
        col("LON")
    )
