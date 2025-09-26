from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def sort_by_campaign_profit_desc(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("campaign_profit").desc())
