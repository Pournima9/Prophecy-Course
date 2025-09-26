from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def Unnest(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("errors.is_error").alias("is_error"), col("errors.error_message").alias("error_message"))
