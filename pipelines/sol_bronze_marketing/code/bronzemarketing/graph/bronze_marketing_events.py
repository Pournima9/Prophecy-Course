from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketing.config.ConfigStore import *
from bronzemarketing.udfs.UDFs import *

def bronze_marketing_events(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .partitionBy("dt")\
        .saveAsTable(f"`rainforest`.`{Config.bronze}`.`marketing_events`")
