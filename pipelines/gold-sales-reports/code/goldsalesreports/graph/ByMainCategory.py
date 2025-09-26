from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def ByMainCategory(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("main_category"))

    return df1.agg(sum(col("total_sales_usd_cents")).alias("total_sales_usd_cents"))
