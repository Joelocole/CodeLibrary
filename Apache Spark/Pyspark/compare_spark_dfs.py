import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, DateType, StringType
import re
from datetime import datetime
import time


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
            ("spark.sql.debug.maxToStringFields", "300"),
            ("spark.executor.memory", "13g")
        ]
    )

    spark_session.sparkContext.stop()
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    spark_session.sparkContext.setCheckpointDir("checkpoint")

    return spark_session


def assert_dataframes_approx_equal(
        df1_path: str, df2_path: str, sep: str = ";", header: bool = None, infer_schema: bool = None,
        spark=init_spark(), sample_fraction: float = 0.10, sample_by_col: str = None, count_nulls: bool = False,
        suffixes: list[str] = ("_OLD", "_NEW"), keep_equal: bool = False,
        index_columns: list[str] = None, detailed_regression: bool = False,
        start_date: str = None, end_date: str = None, ref_date_column: str = None, date_columns: list[str] = None,
        output_csv_path: str = None, abs_tolerance: float = 1.0e-4):

    current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    start_time = time.time()

    def remove_suffixes(input_string, _suffixes):
        # Create a regex pattern to match any of the specified suffixes at the end of the string
        pattern = re.compile(f'({"|".join(_suffixes)})$')

        # Use the sub method to replace the matched pattern with an empty string
        result = re.sub(pattern, '', input_string)

        return result

    df1 = spark.read.csv(df1_path, sep=sep, header=header, inferSchema=infer_schema, nanValue="") \
        if df1_path.endswith('.csv') else spark.read.parquet(df1_path, inferSchema=infer_schema, nanValue="")

    df2 = spark.read.csv(df1_path, sep=sep, header=header, inferSchema=infer_schema, nanValue="") \
        if df1_path.endswith('.csv') else spark.read.parquet(df1_path, inferSchema=infer_schema, nanValue="")

    # STANDARDISE COLUMNS and DATA
    ref_date_column_stand = ref_date_column.upper() if ref_date_column else index_columns[0]
    index_columns_stand = [c.upper() for c in index_columns]

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

    if start_date and end_date and ref_date_column:
        _start_date = f.lit(start_date).cast(DateType())
        _end_date = f.lit(end_date).cast(DateType())

        # Filter df_new
        df1 = df1.filter(f.col(ref_date_column_stand).isNotNull()) \
            .filter((f.col(ref_date_column_stand) >= _start_date) & (f.col(ref_date_column_stand) <= _end_date))

        # Filter df_old
        df2 = df2.filter(f.col(ref_date_column_stand).isNotNull()) \
            .filter((f.col(ref_date_column_stand) >= _start_date) & (f.col(ref_date_column_stand) <= _end_date))

    # COMPARE DFS
    if (df1.count() == df2.count()) & (len(df1.columns) == len(df2.columns)):

        # Select numeric columns to check
        digit_columns = [col.upper() for col in df1.columns if col not in index_columns_stand]

        for c_name in digit_columns:
            df1 = df1.withColumn(
                c_name + suffixes[0],
                f.col(c_name).cast(DoubleType())
                .alias(c_name)).drop(c_name)
            df2 = df2.withColumn(
                c_name + suffixes[1],
                f.col(c_name).cast(DoubleType())
                .alias(c_name)).drop(c_name)

        # Dates, categorical and flag data to string data type
        null_counts_dict = {}
        for c_name in index_columns_stand:
            # if c_name not in date_columns:
            df1 = df1.withColumn(
                c_name,
                f.col(c_name).cast(StringType())
            )
            df2 = df2.withColumn(
                c_name,
                f.col(c_name).cast(StringType())
            )

        if count_nulls:
            for c_name in index_columns_stand:
                sql_table_null_count = \
                    df1.select(f.sum(f.col(c_name).isNull().cast("int")).alias("NullCount")).collect()[0]["NullCount"]
                spark_table_null_count = \
                    df2.select(f.sum(f.col(c_name).isNull().cast("int")).alias("NullCount")).collect()[0]["NullCount"]

                null_counts_dict[f"{c_name}{suffixes[0]}"] = sql_table_null_count
                null_counts_dict[f"{c_name}{suffixes[1]}"] = spark_table_null_count

            for column, count in null_counts_dict.items():
                print(f"Number of nulls in the '{column}' column: {count}")

        print(f"df1 count: \n{df1.count()}")
        print(f"df2 count: \n{df2.count()}")

        # Order DataFrames by index columns
        df1 = df1.orderBy(
            [f.col(c).desc() for c in index_columns_stand],
        )
        df2 = df2.orderBy(
            [f.col(c).desc() for c in index_columns_stand],
        )

        # standardizing Null values in index columns
        for c in index_columns_stand:
            str_fill_value = "@@_test_@@_fill_@@"
            df1 = df1.fillna({c: str_fill_value})
            df2 = df2.fillna({c: str_fill_value})

        if sample_fraction and sample_by_col:
            sample_column = sample_by_col.upper()
            if df1.count() == df2.count():

                print(f"sql table count Before sample: {df1.count()}")
                print(f"spark table count Before sample: {df2.count()}")

                seed_value = 42  # Seed for reproducibility

                # Define the fractions for each category in the stratification column
                # These should be adjusted based on your specific categories and desired sample sizes

                df1_distinct_values = [
                    row[sample_column] for row in df1.select(sample_column).distinct().collect()
                ]

                df2_distinct_values = [
                    row[sample_column] for row in df2.select(sample_column).distinct().collect()
                ]

                # Check if null values exist in the sample_column of each DataFrame
                df1_null_exists = df1.filter(f.col(sample_column).isNull()).count() > 0
                df2_null_exists = df2.filter(f.col(sample_column).isNull()).count() > 0

                # Include null in distinct values if it exists in each DataFrame
                if df1_null_exists:
                    df1_distinct_values.append(None)
                if df2_null_exists:
                    df2_distinct_values.append(None)

                df1_fractions = {
                    val: sample_fraction for val in df1_distinct_values
                }

                df2_fractions = {
                    val: sample_fraction for val in df2_distinct_values
                }

                df1 = df1.stat.sampleBy(sample_column, df1_fractions, seed_value)
                df2 = df2.stat.sampleBy(sample_column, df2_fractions, seed_value)

                print(f"sql table count After sample: {df1.count()}")
                print(f"spark table count After sample: {df2.count()}")
            else:
                raise ValueError(
                    f"dataframe sizes are not identical cannot perform sample\nDataframes shapes df2: {df2.count()}\ndf1: {df1.count()}")

        non_regression_result = df1.join(df2, on=index_columns_stand, how="outer")

        for col in non_regression_result.columns:
            if col not in index_columns_stand:
                col = col[:-len(suffixes[0])] if col.endswith(suffixes[0]) else col[:-len(suffixes[1])]

                if f"{col}_COMPARE" not in non_regression_result.columns:
                    non_regression_result = non_regression_result.withColumn(
                        f"{col}_COMPARE",
                        f.when(
                            (f.col(f"{col}{suffixes[0]}").isNull() & f.col(f"{col}{suffixes[1]}").isNull()) |
                            (f.abs(f.col(f"{col}{suffixes[0]}") - f.col(f"{col}{suffixes[1]}")) < f.lit(abs_tolerance)),
                            True
                        ).otherwise(False)
                    )

        regression_count_dict = {}

        if detailed_regression:
            # detailed regression getting data
            for column in non_regression_result.columns:
                if column.endswith("_COMPARE"):
                    regressions_count = non_regression_result.filter(f.col(column) == False).count()
                    regression_count_dict[column] = regressions_count

            print(f"df1 count: \n{df1.count()}")
            print(f"df2 count: \n{df2.count()}")

            for key, value in regression_count_dict.items():
                print(f"{key}: {value}")

            print(f"non_regression_result count: \n{non_regression_result.count()}")

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
            *index_columns_stand + [c for c in unique_list if c not in index_columns_stand]
        )
        # non_regression_result.show()
        # Filter out rows where a True value is present in columns ending with "_COMPARE"
        if not keep_equal:
            filter_condition = f.expr(
                " OR ".join(f"{col} = False" for col in non_regression_result.columns if col.endswith('_COMPARE')))
            non_regression_result = non_regression_result.filter(filter_condition)

        if not non_regression_result.isEmpty():
            print(f"Dataframes are not identical please check the file {output_csv_path} for a detailed inspection.")
            non_regression_result.show()

            non_regression_result.write \
                .format("csv").option("header", "true").save(
                    f"C:/Users/osahenrunmwencole/Desktop/Prometeia/Projects/BPER_Prepayment_to_sparkSQL/local_tests/{output_csv_path[:-4]}_{current_datetime}.csv"
                )

        # Cleaning columns Suffixes before schema assertion
        df1 = df1.toDF(*[remove_suffixes(col, suffixes) for col in df1.columns])
        df2 = df2.toDF(*[remove_suffixes(col, suffixes) for col in df1.columns])

        assert non_regression_result.count() == 0, "DataFrames are not equal within the specified tolerance"

        assert df1.count() == df2.count(), f"Dfs Counts: Df1: {df1.count()}, Df2: {df2.count()}"

        assert df1.schema == df2.schema, (f"Dataframes schemas are different: "
                                          f"\nDf1 Schema: {df1.printSchema()}"
                                          f"\nDf2 Schema: {df2.printSchema()}")

        if non_regression_result.count() > 0:
            non_regression_result.show()

        print(f"========= ✔️ Test Passed. --- in %s seconds ---" % round((time.time() - start_time), 2), "Dataframes are identical. =========")


    else:
        raise ValueError(
            f"DataFrames have different shapes and cannot be compared."
            f"\nShapes -> Df1: {df1.count(), len(df1.columns)} Df2: {df2.count(), len(df2.columns)}"
            f"\nDf1 columns: {df1.columns} Df2 columns: {df2.columns}"
        )


if __name__ == "__main__":

    index_cols = [
        "dtCutOff", "iSeasoning", "iFlagLastObs", "strPosition", "strServizio", "strBankCode_descr", "strBank_tr", "strCtpSAE_updateG",
        "strCtpSAE_TR_updateG", "strModel", "strFlagParNPar", "strPerNPer", "iClassVol", "strTransAccDA"
    ]
    # ref_date_col = "data_oss"
    date_cols = ["dtcutoff"]

    # MP_GROUPED_DECAY PARAMETERS
    PATH_OLD = "C:/Users/osahenrunmwencole/Downloads/DB_POSTGRE"
    PATH_NEW = "C:/Users/osahenrunmwencole/Downloads/BLOB"

    DB_POSTGRE = PATH_OLD + "/mp_grouped_decay.csv"
    BLOB_SPARK = PATH_NEW + "/mp_grouped_decay.csv/part-00000-01e506a7-3e07-49a9-b718-0322ef954771-c000.csv"

    assert_dataframes_approx_equal(
        DB_POSTGRE, BLOB_SPARK,
        header=True, infer_schema=False, sep=";",
        # abs_tolerance=0.0001,
        index_columns=index_cols, date_columns=date_cols,
        suffixes=("_SQL", "_SPARK"),
        output_csv_path="conflicting_df_MP_GROUPED_DECAY.csv")
