from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def gold_top_main_categories(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable(f"`rainforest`.`{Config.gold}`.`top_main_categories`")
