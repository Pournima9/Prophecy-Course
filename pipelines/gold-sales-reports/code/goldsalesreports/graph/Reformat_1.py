from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("sales_qtr"), 
        col("rank_in_qtr"), 
        col("product_id"), 
        concat(lit("$"), (col("total_sales_usd_cents") / lit(100)).cast(DecimalType(32, 2)).cast(StringType()))\
          .alias("total_sales_usd"), 
        col("name"), 
        col("main_category"), 
        col("sub_category")
    )
