from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *
from prophecy.utils import *
from goldsalesreports.graph import *

def pipeline(spark: SparkSession) -> None:
    df_silver_fct_sales = silver_fct_sales(spark)
    df_bronze_products = bronze_products(spark)
    df_JoinProductId = JoinProductId(spark, df_bronze_products, df_silver_fct_sales)
    df_AddSalesQtr = AddSalesQtr(spark, df_JoinProductId)
    df_ByProductQtr = ByProductQtr(spark, df_AddSalesQtr)
    df_RankByQtr = RankByQtr(spark, df_ByProductQtr)
    df_BySubCategory = BySubCategory(spark, df_AddSalesQtr)
    df_SubCatgTop10Sales = SubCatgTop10Sales(spark, Config.SubCatgTop10Sales, df_BySubCategory)
    gold_top_sub_categories(spark, df_SubCatgTop10Sales)
    df_ByState = ByState(spark, df_AddSalesQtr)
    df_Top10Sales_1 = Top10Sales_1(spark, Config.Top10Sales_1, df_ByState)
    gold_top_state_sales(spark, df_Top10Sales_1)
    df_Top10ByQtr = Top10ByQtr(spark, df_RankByQtr)
    df_Reformat_1 = Reformat_1(spark, df_Top10ByQtr)
    gold_top_products_by_qtr(spark, df_Reformat_1)
    df_ByMainCategory = ByMainCategory(spark, df_BySubCategory)
    df_MainCatTop10Sales = MainCatTop10Sales(spark, Config.MainCatTop10Sales, df_ByMainCategory)
    gold_top_main_categories(spark, df_MainCatTop10Sales)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gold-sales-reports")
    spark.conf.set("spark.sql.shuffle.partitions", "10")
    spark.conf.set("spark.shuffle.partitions", "11")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gold-sales-reports", config = Config)(pipeline)

if __name__ == "__main__":
    main()
