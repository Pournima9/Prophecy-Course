from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def raw_sales(spark: SparkSession) -> DataFrame:
    return spark.read.format("json").load("dbfs:/course_lab/rainforest_raw_data/sales_events/*")
