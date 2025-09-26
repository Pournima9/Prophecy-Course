from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketing.config.ConfigStore import *
from bronzemarketing.udfs.UDFs import *

def write_marketing_event(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .partitionBy("dt")\
        .saveAsTable("`rainforest`.`bronze_c3782efe`.`cleaned_marketing_event`")
