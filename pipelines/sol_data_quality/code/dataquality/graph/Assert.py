from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def Assert(spark: SparkSession, in0: DataFrame):
    error_coumt = in0.count()
    assert error_coumt == 0, f"{error_coumt} data errors detected"

    return 
