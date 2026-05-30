from pyspark.sql import SparkSession
from pyspark.sql.fuctions import col, expr, row_number, max as spark_max
from pyspark.sql.window import Window
from datetime import datetime, timedelta

#SparkSession
spark = SparkSession.builder.\
             appName("BRONZE_To_Silver_Generic_Driver").\
             getOrCreate()


# Helpes
def read_

