from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def CleanPrice(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("EAN13"), 
        col("EAN5"), 
        col("price"), 
        col("quantity"), 
        when(col("price").rlike("\\$(\\s)?\\d+"), lit("USD"))\
          .when(col("price").rlike("USD(\\s)?\\d+"), lit("USD"))\
          .when(col("price").rlike("YEN(\\s)?\\d+"), lit("YEN"))\
          .when(col("price").rlike("EUR(\\s)?\\d+"), lit("EUR"))\
          .otherwise(lit("UNKNOWN"))\
          .alias("currency_type"), 
        when(col("price").rlike("\\$(\\s)?\\d+"), regexp_extract(col("price"), "\\$(\\s)?(\\d+)", 2).cast(IntegerType()))\
          .when(col("price").rlike("USD(\\s)?\\d+"), regexp_extract(col("price"), "USD(\\s)?(\\d+)", 2).cast(IntegerType()))\
          .when(col("price").rlike("YEN(\\s)?\\d+"), regexp_extract(col("price"), "YEN(\\s)?(\\d+)", 2).cast(IntegerType()))\
          .when(col("price").rlike("EUR(\\s)?\\d+"), regexp_extract(col("price"), "EUR(\\s)?(\\d+)", 2).cast(IntegerType()))\
          .otherwise(lit(None).cast(IntegerType()))\
          .alias("currency_amount")
    )
