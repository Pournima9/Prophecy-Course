from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def campaign_revenue_summary(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("campaign_quarter"))

    return df1.agg(
        (sum(col("campaign_cost_micros_usd")) / pow(lit(10), lit(6)))\
          .cast(DecimalType(30, 2))\
          .alias("campaign_cost_usd"), 
        (sum(col("revenue_generated_micros_usd")) / pow(lit(10), lit(6)))\
          .cast(DecimalType(30, 2))\
          .alias("revenue_generated_usd")
    )
