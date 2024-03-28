import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, DateType, StringType


def init_spark():
    # SPARK INITIALISATION
    # Defining Environment Variables
    os.environ['SPARK_DRIVER_MEMORY'] = "8G"
    os.environ['SPARK_EXECUTOR_CORES'] = "6"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/spark/hadoop335"
    os.environ['SPARK_HOME'] = "C:/spark/spark342"
    sys.path.append("C:/spark/hadoop335/bin")

    # Creating Spark Session
    spark_session = SparkSession.builder.getOrCreate()

    print(f"Spark version: {spark_session.version}")
    conf = spark_session.sparkContext._conf.setAll(
        [
            ('spark.app.name', 'APP Prepayment | Local - Tests'),
            ("spark.sql.execution.arrow.pyspark.enabled", "true"),
            ('spark.driver.maxResultSize', "8G"),
            ("spark.memory.offHeap.enabled", "true"),
            ('spark.memory.offHeap.size', '8G'),
            ("spark.sql.shuffle.partitions", "10"),
            ("spark.sql.adaptive.enabled", "true"),
            ("spark.sql.autoBroadcastJoinThreshold", "-1"),
            ("spark.sql.debug.maxToStringFields", "250"),
            ("spark.executor.memory", "11g"),
            # ("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        ]
    )

    spark_session.sparkContext.stop()
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    spark_session.sparkContext.setCheckpointDir("checkpoint")

    return spark_session



