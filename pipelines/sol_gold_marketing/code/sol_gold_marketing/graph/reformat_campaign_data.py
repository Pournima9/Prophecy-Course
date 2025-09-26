from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def reformat_campaign_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(year(to_date(col("campaign_start"))), lit("Q"), quarter(to_date(col("campaign_start"))))\
          .alias("campaign_quarter"), 
        col("campaign_cost_micros_usd"), 
        col("revenue_generated_micros_usd")
    )
