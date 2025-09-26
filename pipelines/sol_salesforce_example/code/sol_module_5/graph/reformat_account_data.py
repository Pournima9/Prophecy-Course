from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *

def reformat_account_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("Id").alias("account_id"), col("Type").alias("account_type"))
