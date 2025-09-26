from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *
from prophecy.utils import *
from bulkloadbronzeexample.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_sales = raw_sales(spark)
    bronze_sales(spark, df_raw_sales)
    df_raw_products = raw_products(spark)
    df_customer_snapshots_all = customer_snapshots_all(spark)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_customer_snapshots_all)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SchemaTransform_1)
    df_WindowFunction_1 = WindowFunction_1(spark, df_Deduplicate_1)
    df_Reformat_2 = Reformat_2(spark, df_WindowFunction_1)
    bronze_customers(spark, df_Reformat_2)
    bronze_products(spark, df_raw_products)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/bulk-load-bronze-example")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/bulk-load-bronze-example", config = Config)(pipeline)

if __name__ == "__main__":
    main()
