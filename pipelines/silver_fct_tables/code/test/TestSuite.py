import unittest

from test.silver_fct_tables.graph.test_JoinDimensions import *
from test.silver_fct_tables.graph.test_Reformat_1 import *
from test.silver_fct_tables.graph.test_FormatSales import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
