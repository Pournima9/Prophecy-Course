from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def ConvertCurrency(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "price_usd_cents",
        when((col("currency_type") == lit("USD")), (lit(100) * col("currency_amount")).cast(LongType()))\
          .when((col("currency_type") == lit("YEN")), round((lit(0.67) * col("currency_amount"))).cast(LongType()))\
          .when((col("currency_type") == lit("EUR")), (lit(106) * col("currency_amount")).cast(LongType()))\
          .otherwise(lit(None).cast(LongType()))
    )
