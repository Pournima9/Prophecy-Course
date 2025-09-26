from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *

def filter_current(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(col("is_current"))
