from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def raw_sales_increment(spark: SparkSession) -> DataFrame:
    return spark.read.format("json").load(f"{Config.source_path}/sales_events/{Config.dt}")
