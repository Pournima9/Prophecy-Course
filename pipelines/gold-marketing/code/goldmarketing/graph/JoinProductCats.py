from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def JoinProductCats(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.product_id") == col("in1.product_id")), "inner")\
        .select(col("in0.campaign_id").alias("campaign_id"), col("in0.campaign_start").alias("campaign_start"), col("in0.campaign_end").alias("campaign_end"), col("in0.product_id").alias("product_id"), col("in0.campaign_cost_micros_usd").alias("campaign_cost_micros_usd"), col("in0.revenue_generated_micros_usd").alias("revenue_generated_micros_usd"), col("in1.main_category").alias("main_category"), col("in1.sub_category").alias("sub_category"))
