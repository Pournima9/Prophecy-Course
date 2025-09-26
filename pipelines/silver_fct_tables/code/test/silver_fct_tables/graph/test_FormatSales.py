from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from silver_fct_tables.graph.FormatSales import *
from silver_fct_tables.config.ConfigStore import *


class FormatSalesTest(BaseTestCase):

    def test_testsalesid(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/FormatSales/in0/schema.json',
            'test/resources/data/silver_fct_tables/graph/FormatSales/in0/data/test_testsalesid.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/FormatSales/out/schema.json',
            'test/resources/data/silver_fct_tables/graph/FormatSales/out/data/test_testsalesid.json',
            'out'
        )
        dfOutComputed = FormatSales(self.spark, dfIn0)
        assertDFEquals(dfOut.select("sales_id"), dfOutComputed.select("sales_id"), self.maxUnequalRowsToShow)

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
