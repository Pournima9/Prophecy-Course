from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from silver_fct_tables.graph.Reformat_1 import *
from silver_fct_tables.config.ConfigStore import *


class Reformat_1Test(BaseTestCase):

    def test_testrandproductid(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/Reformat_1/in0/schema.json',
            'test/resources/data/silver_fct_tables/graph/Reformat_1/in0/data/test_testrandproductid.json',
            'in0'
        )
        dfOutComputed = Reformat_1(self.spark, dfIn0)
        assertPredicates(
            "out",
            dfOutComputed,
            list(zip([(col("rand_product_id") < col("product_id"))], ["max_product_id"]))
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
