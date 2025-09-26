from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from goldsalesreports.udfs.UDFs import *
from . import *
from .config import *

def Top10Sales_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_OrderBySales = OrderBySales(spark, in0)
    df_Top10 = Top10(spark, df_OrderBySales)
    df_PrettyUSD = PrettyUSD(spark, df_Top10)
    subgraph_config.update(Config)

    return df_PrettyUSD
