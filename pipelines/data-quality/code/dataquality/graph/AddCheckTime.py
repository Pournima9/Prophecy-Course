from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def AddCheckTime(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(current_timestamp().alias("checked_at"), col("is_error"), col("error_message"))
