from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *
from prophecy.utils import *
from sol_module_6.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_customers = bronze_customers(spark)
    df_filter_current = filter_current(spark, df_bronze_customers)
    df_bronze_sales = bronze_sales(spark)
    df_join_by_customer_id = join_by_customer_id(spark, df_filter_current, df_bronze_sales)
    df_total_price_by_postcode = total_price_by_postcode(spark, df_join_by_customer_id)
    df_reformat_total_price_and_postcode = reformat_total_price_and_postcode(spark, df_total_price_by_postcode)
    sol_sales_by_postcode(spark, df_reformat_total_price_and_postcode)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sol_sales_by_postcode")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/sol_sales_by_postcode", config = Config)(pipeline)

if __name__ == "__main__":
    main()
