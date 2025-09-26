from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from goldsalesreports.udfs.UDFs import *

def Top10(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(10)
