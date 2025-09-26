from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def campaign_performance(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("campaign_quarter"), 
        col("campaign_cost_usd"), 
        col("revenue_generated_usd"), 
        (col("revenue_generated_usd") - col("campaign_cost_usd")).alias("campaign_profit")
    )
