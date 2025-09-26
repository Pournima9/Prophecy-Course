from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silvermarketing.config.ConfigStore import *
from silvermarketing.udfs.UDFs import *

def SumRevenue(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("campaign_id"))

    return df1.agg(sum(col("price_usd_cents")).alias("revenue_generated_usd_cents"))
