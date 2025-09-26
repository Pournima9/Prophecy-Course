from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *

def total_price_by_postcode(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("postcode"))

    return df1.agg(sum(col("total_price_usd_cents")).alias("total_price_usd_cents"))
