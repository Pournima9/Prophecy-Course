from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def CountFields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        countDistinct(col("sales_id")).alias("distinct_sales_id"), 
        count(col("sales_id")).alias("sales_id_count"), 
        sum(expr("if((sales_id IS NULL), 1, 0)")).alias("sales_id_null_count"), 
        sum(expr("if((customer_id IS NULL), 1, 0)")).alias("customer_id_null_count"), 
        sum(expr("if((product_id IS NULL), 1, 0)")).alias("product_id_null_count")
    )
