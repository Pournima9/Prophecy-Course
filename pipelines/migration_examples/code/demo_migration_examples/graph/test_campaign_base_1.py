from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def test_campaign_base_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.silver}`.`test_campaign_base`")
