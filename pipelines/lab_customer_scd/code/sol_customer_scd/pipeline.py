from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_customer_scd.config.ConfigStore import *
from sol_customer_scd.udfs.UDFs import *
from prophecy.utils import *
from sol_customer_scd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_customer_snapshot = raw_customer_snapshot(spark)
    df_bronze_customers = bronze_customers(spark)
    df_filter_by_valid_date = filter_by_valid_date(spark, df_bronze_customers)
    df_reformatted_customer_info = reformatted_customer_info(spark, df_filter_by_valid_date)
    df_reformatted_customer_info_1 = reformatted_customer_info_1(spark, df_raw_customer_snapshot)
    df_set_difference = set_difference(spark, df_reformatted_customer_info_1, df_reformatted_customer_info)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/lab_customer_scd")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/lab_customer_scd", config = Config)(pipeline)

if __name__ == "__main__":
    main()
