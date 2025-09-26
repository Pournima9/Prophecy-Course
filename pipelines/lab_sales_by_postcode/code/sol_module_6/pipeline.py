from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *
from prophecy.utils import *
from sol_module_6.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_sales = bronze_sales(spark)
    df_bronze_customers = bronze_customers(spark)
    df_filter_current = filter_current(spark, df_bronze_customers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/lab_sales_by_postcode")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/lab_sales_by_postcode", config = Config)(pipeline)

if __name__ == "__main__":
    main()
