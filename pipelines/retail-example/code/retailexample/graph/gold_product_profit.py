from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def gold_product_profit(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable(f"`rainforest`.`{Config.gold}`.`product_profit`")
