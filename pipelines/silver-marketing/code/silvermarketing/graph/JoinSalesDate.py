from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silvermarketing.config.ConfigStore import *
from silvermarketing.udfs.UDFs import *

def JoinSalesDate(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.dt") == to_date(col("in1.sale_date"))) & (col("in0.product_id") == col("in1.product_id"))),
          "inner"
        )\
        .select(col("in1.price_usd_cents").alias("price_usd_cents"), col("in0.campaign_id").alias("campaign_id"), col("in0.product_id").alias("product_id"), col("in0.dt").alias("dt"), col("in1.sales_id").alias("sales_id"))
