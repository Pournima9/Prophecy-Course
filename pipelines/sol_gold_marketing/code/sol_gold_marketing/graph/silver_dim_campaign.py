from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def silver_dim_campaign(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.silver}`.`dim_campaign`")
