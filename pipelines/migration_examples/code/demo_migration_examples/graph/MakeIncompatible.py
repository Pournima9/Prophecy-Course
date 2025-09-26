from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def MakeIncompatible(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("campaign_id").cast(StringType()).alias("campaign_id"), 
        col("campaign_start"), 
        col("campaign_end"), 
        col("product_id"), 
        col("campaign_cost_micros_usd"), 
        col("revenue_generated_micros_usd"), 
        current_timestamp().alias("as_of")
    )
