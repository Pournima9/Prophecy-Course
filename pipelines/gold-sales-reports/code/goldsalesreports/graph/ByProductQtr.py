from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def ByProductQtr(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("sales_qtr"), col("product_id"))

    return df1.agg(
        sum(col("price_usd_cents")).alias("total_sales_usd_cents"), 
        first(col("name")).alias("name"), 
        first(col("main_category")).alias("main_category"), 
        first(col("sub_category")).alias("sub_category")
    )
