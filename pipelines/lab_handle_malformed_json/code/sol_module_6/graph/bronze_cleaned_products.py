from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *

def bronze_cleaned_products(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .partitionBy("date_introduced")\
        .saveAsTable("`rainforest`.`bronze_c3782efe`.`cleaned_products`")
