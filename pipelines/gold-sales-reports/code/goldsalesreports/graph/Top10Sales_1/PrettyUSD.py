from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from goldsalesreports.udfs.UDFs import *

def PrettyUSD(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "total_sales_usd",
          concat(lit("$"), (col("total_sales_usd_cents") / lit(100)).cast(DecimalType(32, 2)).cast(StringType()))
        )\
        .drop("total_sales_usd_cents")
