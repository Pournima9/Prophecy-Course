from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def raw_products(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/course_lab/rainforest_raw_data/products_parquet/*")
