from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def DeltaTableOperations_1(spark: SparkSession):
    if not ("SColumnExpression" in locals()):
        from delta.tables import DeltaTable
        DeltaTable.forName(spark, "`{}`.`test_campaign_base`".format(f"rainforest.{Config.silver}")).restoreToVersion(0)
