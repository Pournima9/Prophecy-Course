from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def test_campaign_incompatible(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .mode("append")\
        .saveAsTable(f"`rainforest`.`{Config.silver}`.`test_campaign_base`")
