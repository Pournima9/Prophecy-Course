from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def RankByQtr(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "rank_in_qtr",
        rank().over(Window.partitionBy(col("sales_qtr")).orderBy(col("total_sales_usd_cents").desc()))
    )
