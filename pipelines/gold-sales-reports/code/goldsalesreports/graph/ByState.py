from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def ByState(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("customer_state"))

    return df1.agg(sum(col("price_usd_cents")).alias("total_sales_usd_cents"))
