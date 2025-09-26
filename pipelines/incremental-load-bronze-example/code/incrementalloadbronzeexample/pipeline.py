from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *
from prophecy.utils import *
from incrementalloadbronzeexample.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_sales = bronze_sales(spark)
    df_raw_customer_snapshot = raw_customer_snapshot(spark)
    df_AddScd2Fields = AddScd2Fields(spark, df_raw_customer_snapshot)
    bronze_customers_increment(spark, df_AddScd2Fields)
    df_raw_products_increment = raw_products_increment(spark)
    df_raw_sales_increment = raw_sales_increment(spark)
    bronze_products_increment(spark, df_raw_products_increment)
    bronze_sales_increment(spark, df_raw_sales_increment)
    df_bronze_products = bronze_products(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/incremental-load-bronze-example")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/incremental-load-bronze-example", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
