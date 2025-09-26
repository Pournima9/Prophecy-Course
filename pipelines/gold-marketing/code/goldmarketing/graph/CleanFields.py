from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def CleanFields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("main_category"), 
        col("sub_category"), 
        col("campaign_start"), 
        col("num_campaigns"), 
        (col("campaign_cost_micros_usd") * pow(lit(10), lit(-6))).cast(DecimalType(32, 2)).alias("cost_usd"), 
        (col("revenue_generated_micros_usd") * pow(lit(10), lit(-6)))\
          .cast(DecimalType(32, 2))\
          .alias("revenue_generated_usd"), 
        ((col("revenue_generated_micros_usd") - col("campaign_cost_micros_usd")) * pow(lit(10), lit(-6)))\
          .cast(DecimalType(32, 2))\
          .alias("revenue_sub_marketing_usd")
    )
