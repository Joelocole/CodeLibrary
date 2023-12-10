import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, DateType
import re
from datetime import datetime


def init_spark():

    # SPARK INITIALISATION
    # Defining Environment Variables
    os.environ['SPARK_DRIVER_MEMORY'] = "8G"
    os.environ['SPARK_EXECUTOR_CORES'] = "6"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/spark/hadoop33"
    os.environ['SPARK_HOME'] = "C:/spark/spark34"
    sys.path.append("C:/spark/hadoop33/bin")

    # Creating Spark Session
    spark_session = SparkSession.builder.getOrCreate()

    print(f"Spark version: {spark_session.version}")
    conf = spark_session.sparkContext._conf.setAll(
        [
            ('spark.app.name', 'BPER Prepayment | Local - Tests'),
            ("spark.sql.execution.arrow.pyspark.enabled", "true"),
            ('spark.driver.maxResultSize', "8G"),
            ("spark.memory.offHeap.enabled", "true"),
            ('spark.memory.offHeap.size', '8G'),
            ("spark.sql.shuffle.partitions", "6"),
            ("spark.sql.adaptive.enabled", "true"),
            ("spark.sql.autoBroadcastJoinThreshold", "-1"),
            ("spark.sql.debug.maxToStringFields", "500"),
            ("spark.executor.memory", "12g")
        ]
    )

    spark_session.sparkContext.stop()
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    spark_session.sparkContext.setCheckpointDir("checkpoint")

    return spark_session


def assert_dataframes_approx_equal(
        df1_path, df2_path, sep=";", header=None, inferSchema=None, spark=init_spark(), suffixes=("_OLD", "_NEW"),
        index_columns=None, start_date=None, end_date=None, ref_date_column=None, date_columns=None,
        output_csv_path=None, abs_tolerance=1.0e-4):

    current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    def remove_suffixes(input_string, suffixes):
        # Create a regex pattern to match any of the specified suffixes at the end of the string
        pattern = re.compile(f'({"|".join(suffixes)})$')

        # Use the sub method to replace the matched pattern with an empty string
        result = re.sub(pattern, '', input_string)

        return result

    def approx_spark(value, abs_tolerance):
         return f.when(f.isnull(value) | (f.abs(value) >= abs_tolerance), value).otherwise(None)

    index_columns = [c.upper() for c in index_columns]

    df1 = spark.read.csv(df1_path, sep=sep, header=header, inferSchema=inferSchema,  nanValue="")
    df2 = spark.read.csv(df2_path, sep=sep, header=header, inferSchema=inferSchema,  nanValue="")

    # Convert column names to uppercase
    for col_name in df1.columns:
        df1 = df1.withColumnRenamed(col_name, col_name.upper())

    for col_name in df2.columns:
        df2 = df2.withColumnRenamed(col_name, col_name.upper())

    if date_columns:
        date_columns = [date_col.upper() for date_col in date_columns]
        for date_col in date_columns:
            df1 = df1.withColumn(
                date_col, f.to_date(f.substring(f.regexp_replace(f.col(date_col), "-", ""), 0, 8), 'yyyyMMdd')
            )
            df2 = df2.withColumn(
                date_col, f.to_date(f.substring(f.regexp_replace(f.col(date_col), "-", ""), 0, 8), 'yyyyMMdd')
            )

    ref_date_column = ref_date_column.upper()
    if start_date and end_date and ref_date_column:

        start_date = f.lit(start_date).cast(DateType())
        end_date = f.lit(end_date).cast(DateType())

        # Filter df_new
        df1 = df1.filter(f.col(ref_date_column).isNotNull()) \
            .filter((f.col(ref_date_column) >= start_date) & (f.col(ref_date_column) <= end_date))

        # Filter df_old
        df2 = df2.filter(f.col(ref_date_column).isNotNull()) \
            .filter((f.col(ref_date_column) >= start_date) & (f.col(ref_date_column) <= end_date))

    # COMPARE DFS
    if ~(df1.count() == df2.count()) & ~(len(df1.columns) == len(df2.columns) & ~(df1.columns == df2.columns)):

        # Select numeric columns to check
        digit_columns = [col.upper() for col in df1.columns if col not in index_columns]

        for c_name in digit_columns:
            df1 = df1.withColumn(
                c_name + suffixes[0],
                f.col(c_name).cast(DoubleType())
                .alias(c_name)).drop(c_name)
            df2 = df2.withColumn(
                c_name + suffixes[1],
                f.col(c_name).cast(DoubleType())
                .alias(c_name)).drop(c_name)

        # Order DataFrames by index columns
        df1 = df1.orderBy(*[f.col(col).desc() for col in index_columns])
        df2 = df2.orderBy(*[f.col(col).desc() for col in index_columns])

        # List of numerical columns for comparison
        suffixes_0_list = [f"{col}{suffixes[0]}" for col in digit_columns]
        suffixes_1_list = [f"{col}{suffixes[1]}" for col in digit_columns]

        for col_name in digit_columns:
            df1 = df1.withColumn(
                f"{col_name}{suffixes[0]}",
                approx_spark(f.col(f"{col_name}{suffixes[0]}"), f.lit(abs_tolerance))
            ).alias(f"{col_name}{suffixes[0]}")
            df2 = df2.withColumn(
                f"{col_name}{suffixes[1]}",
                approx_spark(f.col(f"{col_name}{suffixes[1]}"), f.lit(abs_tolerance))
            ).alias(f"{col_name}{suffixes[1]}")

        non_regression_result = df1.join(df2, on=index_columns, how="outer")

        for col in non_regression_result.columns:
            if col not in index_columns:
                col = col[:-len(suffixes[0])] if col.endswith(suffixes[0]) else col[:-len(suffixes[1])]

                if f"{col}_COMPARE" not in non_regression_result.columns:
                    non_regression_result = non_regression_result.withColumn(
                        f"{col}_COMPARE",
                        f.when(
                            (f.col(f"{col}{suffixes[0]}").isNull() & f.col(f"{col}{suffixes[1]}").isNull()) |
                            (f.col(f"{col}{suffixes[0]}") == f.col(f"{col}{suffixes[1]}")),
                            True
                        ).otherwise(False)
                    )

        # for col in non_regression_result.columns:
        #     # suff_x = [suff for suff in suffixes]
        #     if col not in index_columns:
        #         if col.endswith('_COMPARE'):
        #             non_regression_result = non_regression_result.filter(
        #                 ~f.expr(f"{col}")
        #             )

        temp_cols = [col for col in non_regression_result.columns]
        prefixes = list(set(remove_suffixes(col, suffixes) for col in temp_cols))
        prefixes.sort()

        ordered_columns = []
        # Loop through prefixes and order columns for each prefix
        for prefix in prefixes:
            for col_name in temp_cols:
                if col_name.startswith(prefix):
                    ordered_columns.append(col_name)

        # remove duplicate columns
        seen = set()
        unique_list = [x for x in ordered_columns if x not in seen and not seen.add(x)]

        non_regression_result = non_regression_result.select(
            *[ref_date_column] + [c for c in unique_list if c != ref_date_column]
        )

        # Filter out rows where a True value is present in columns ending with "_COMPARE"
        filter_condition = f.expr(
            " OR ".join(f"{col} = False" for col in non_regression_result.columns if col.endswith('_COMPARE')))
        non_regression_result = non_regression_result.filter(filter_condition)

        if not non_regression_result.isEmpty():
            print(f"Dataframes are not identical please check the file {output_csv_path} for a detailed inspection.")
            non_regression_result.show(non_regression_result.count())

            non_regression_result.toPandas().to_csv(
                f"{output_csv_path[:-4]}_{current_datetime}.csv", index=True, sep=";", encoding='utf-8'
            )

        # Cleaning columns Suffixes before schema assertion
        df1 = df1.toDF(*[remove_suffixes(col, suffixes) for col in df1.columns])
        df2 = df2.toDF(*[remove_suffixes(col, suffixes) for col in df1.columns])

        assert non_regression_result.count() == 0, "DataFrames are not equal within the specified tolerance"

        assert df1.count() == df2.count(), f"Dfs Counts: Df1: {df1.count()}, Df2: {df2.count()}"

        assert df1.schema == df2.schema, f"Dataframes schemas are different: \nDf1 Schema: {df1.printSchema()}\nDf2 Schema: {df2.printSchema()}"

        if non_regression_result.count() > 0:
            non_regression_result.show()

        print("=== ✔️ Test Passed. Dataframes are identical. ===")
    else:
        raise ValueError(
            f"DataFrames have different shapes and cannot be compared."
            f"\nShapes -> Df1: {df1.count(), len(df1.columns)} Df2: {df2.count(), len(df2.columns)}"
            f"\nDf1 columns: {df1.columns} Df2 columns: {df2.columns}"
        )


if __name__ == "__main__":
    df1_path = "C:/Users/osahenrunmwencole/Downloads/tassi_varmacro_mp.csv"
    df2_path = "C:/Users/osahenrunmwencole/Desktop/Prometeia/Projects/BPER_Prepayment_to_sparkSQL/local_tests/tassi_varmacro_local.csv"

    index_columns = ["data_oss"]

    assert_dataframes_approx_equal(
        df1_path, df2_path, header=True, inferSchema=False, sep=";", abs_tolerance=0.0001,
        index_columns=index_columns, date_columns=["data_oss"], suffixes=("_SQL", "_SPARK"), output_csv_path="conflicting_df.csv",
        ref_date_column="data_oss", start_date="1990-01-31", end_date="2022-09-30")
