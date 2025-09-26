from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketing.config.ConfigStore import *
from bronzemarketing.udfs.UDFs import *

def raw_marketing_events(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/course_lab/rainforest_raw_data/marketing_events/*")
