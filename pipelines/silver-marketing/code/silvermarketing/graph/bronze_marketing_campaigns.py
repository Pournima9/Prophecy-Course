from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silvermarketing.config.ConfigStore import *
from silvermarketing.udfs.UDFs import *

def bronze_marketing_campaigns(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.bronze}`.`marketing_campaigns`")
