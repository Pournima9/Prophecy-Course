from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def MakeIncompatible_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("campaign_id", col("campaign_id").cast(StringType())).withColumn("as_of", current_timestamp())
