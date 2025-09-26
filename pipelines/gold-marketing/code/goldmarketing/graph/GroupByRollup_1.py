from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def GroupByRollup_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.rollup(col("main_category"), col("sub_category"), col("campaign_start"))

    return df1.agg(
        count(lit(1)).alias("num_campaigns"), 
        sum(col("campaign_cost_micros_usd")).alias("campaign_cost_micros_usd"), 
        sum(col("revenue_generated_micros_usd")).alias("revenue_generated_micros_usd")
    )
