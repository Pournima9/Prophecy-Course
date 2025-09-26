from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def AddScd2Fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        col("signup_date"), 
        to_date(lit(Config.dt)).alias("valid_from"), 
        lit(None).cast(DateType()).alias("valid_to"), 
        lit(True).alias("is_original"), 
        lit(True).alias("is_current"), 
        col("customer_name"), 
        col("phone_number"), 
        col("street_number"), 
        col("street_name"), 
        col("unit"), 
        col("city"), 
        col("state"), 
        col("postcode"), 
        col("lat"), 
        col("lon")
    )
