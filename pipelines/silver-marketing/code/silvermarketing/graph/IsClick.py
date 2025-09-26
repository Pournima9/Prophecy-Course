from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silvermarketing.config.ConfigStore import *
from silvermarketing.udfs.UDFs import *

def IsClick(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("click") == lit("1")))
