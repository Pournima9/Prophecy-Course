from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def sales_dq_checks(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("append").saveAsTable(f"`rainforest`.`{Config.gold}`.`sales_dq_checks`")
