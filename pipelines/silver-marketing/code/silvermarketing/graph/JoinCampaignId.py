from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silvermarketing.config.ConfigStore import *
from silvermarketing.udfs.UDFs import *

def JoinCampaignId(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.campaign_id") == col("in1.campaign_id")), "outer")\
        .join(in2.alias("in2"), (col("in0.campaign_id") == col("in2.campaign_id")), "outer")\
        .select(col("in2.campaign_id").alias("campaign_id"), col("in2.campaign_start").alias("campaign_start"), col("in2.campaign_end").alias("campaign_end"), col("in2.product_id").alias("product_id"), col("in0.campaign_cost_micros_usd").alias("campaign_cost_micros_usd"), (coalesce(col("in1.revenue_generated_usd_cents"), lit(0)) * pow(lit(10), lit(4))).alias(
        "revenue_generated_micros_usd"
    ))
