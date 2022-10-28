import pytest

import pyspark
import spreadth
import sys

class TestSpreadth:

    def test_incorrect_construction(self):
        try:
            s = spreadth.Spreadth()
            assert False
        except:
            assert True

    def test_correct_construction(self):
        try:
            spark = pyspark.sql.SparkSession.builder.getOrCreate()
            s = spreadth.Spreadth(spark.createDataFrame([(1,"foo"),(2,"bar")]))
            assert True
        except:
            assert False