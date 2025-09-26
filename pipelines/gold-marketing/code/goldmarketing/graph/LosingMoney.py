from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def LosingMoney(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("campaign_cost_micros_usd") > col("revenue_generated_micros_usd")))
