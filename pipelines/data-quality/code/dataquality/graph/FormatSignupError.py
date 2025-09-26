from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def FormatSignupError(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        (col("mismatch_count") > lit(0)).alias("is_error"), 
        lit("customer signup_dates are changing for the same customer").alias("error_message")
    )
