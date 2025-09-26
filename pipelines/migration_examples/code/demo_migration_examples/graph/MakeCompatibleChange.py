from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def MakeCompatibleChange(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("campaign_id"), 
        lit("Buy this product please").alias("campaign_slogan"), 
        current_timestamp().alias("as_of")
    )
