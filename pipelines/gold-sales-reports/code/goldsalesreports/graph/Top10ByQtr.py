from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def Top10ByQtr(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("rank_in_qtr") <= lit(10)))
