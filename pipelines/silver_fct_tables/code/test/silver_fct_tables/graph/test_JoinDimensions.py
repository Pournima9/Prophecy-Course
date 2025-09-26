from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from silver_fct_tables.graph.JoinDimensions import *
from silver_fct_tables.config.ConfigStore import *


class JoinDimensionsTest(BaseTestCase):

    def test_testfulladdress(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in0/schema.json',
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in0/data/test_testfulladdress.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in1/schema.json',
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in1/data/test_testfulladdress.json',
            'in1'
        )
        dfIn2 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in2/schema.json',
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/in2/data/test_testfulladdress.json',
            'in2'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/out/schema.json',
            'test/resources/data/silver_fct_tables/graph/JoinDimensions/out/data/test_testfulladdress.json',
            'out'
        )
        dfOutComputed = JoinDimensions(self.spark, dfIn0, dfIn1, dfIn2)
        assertDFEquals(
            dfOut.select("shipping_full_address"),
            dfOutComputed.select("shipping_full_address"),
            self.maxUnequalRowsToShow
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
