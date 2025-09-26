from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *
from prophecy.utils import *
from retailexample.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sales_orders_base_1 = sales_orders_base_1(spark)
    df_FlattenOrders = FlattenOrders(spark, df_sales_orders_base_1)
    df_RenameFields = RenameFields(spark, df_FlattenOrders)
    df_purchase_orders_base = purchase_orders_base(spark)
    df_CleanPrice = CleanPrice(spark, df_purchase_orders_base)
    df_ConvertCurrency = ConvertCurrency(spark, df_CleanPrice)
    df_products_base_1 = products_base_1(spark)
    df_AvgCost = AvgCost(spark, df_ConvertCurrency)
    df_JoinProductCost = JoinProductCost(spark, df_products_base_1, df_AvgCost)
    df_SumSales = SumSales(spark, df_RenameFields)
    df_JoinProduct = JoinProduct(spark, df_SumSales, df_JoinProductCost)
    df_CalcProfit = CalcProfit(spark, df_JoinProduct)
    df_Finalize = Finalize(spark, df_CalcProfit)
    gold_product_profit(spark, df_Finalize)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/retail-example")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/retail-example", config = Config)(pipeline)

if __name__ == "__main__":
    main()
