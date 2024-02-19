import os
import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, DateType, StringType
import re
from datetime import datetime


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


spark = init_spark()


def assert_dataframes_approx_equal(
        df1, df2,
        suffixes: list[str] = ("_OLD", "_NEW"), count_nulls: bool = False, keep_equal: bool = False,
        index_columns: list[str] = None, show_detailed_regression: bool = False,
        start_date: str = None, end_date: str = None, ref_date_column: str = None, date_columns: list[str] = None,
        abs_tolerance: float = 1.0e-4):

    def remove_suffixes(input_string, _suffixes):
        # Create a regex pattern to match any of the specified suffixes at the end of the string
        pattern = re.compile(f'({"|".join(_suffixes)})$')

        # Use the sub method to replace the matched pattern with an empty string
        result = re.sub(pattern, '', input_string)

        return result

    # STANDARDIZE COLUMNS
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
            null_counts_dict = {}
            for c_name in index_columns_stand:
                sql_table_null_count = \
                df1.select(f.sum(f.col(c_name).isNull().cast("int")).alias("NullCount")).collect()[0]["NullCount"]
                spark_table_null_count = \
                df2.select(f.sum(f.col(c_name).isNull().cast("int")).alias("NullCount")).collect()[0]["NullCount"]
                null_counts_dict[f"{c_name}_SQL"] = sql_table_null_count
                null_counts_dict[f"{c_name}_SPARK"] = spark_table_null_count

            for column, count in null_counts_dict.items():
                print(f"Number of nulls in the '{column}' column: {count}")

        # Order DataFrames by
        df1 = df1.orderBy(
            [f.col(c).desc() for c in index_columns_stand]
        )
        df2 = df2.orderBy(
            [f.col(c).desc() for c in index_columns_stand]
        )

        # standardizing null values in join keys
        str_fill_value = "@@_test_@@_fill_@@"
        fill_values = {c: str_fill_value for c in index_columns_stand}
        df1 = df1.fillna(fill_values)
        df2 = df2.fillna(fill_values)

        print(f"df1 count: {df1.count()}")
        print(f"df2 count: {df2.count()}")

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

        if show_detailed_regression:
            regression_count_dict = {}

            for column in non_regression_result.columns:
                if column.endswith("_COMPARE"):
                    regressions_count = non_regression_result.filter(f.col(column) == False).count()
                    regression_count_dict[column] = regressions_count

            for key, value in regression_count_dict.items():
                print(f"{key}: {value}")

            print(f"non_regression_result_df count: \n{non_regression_result.count()}")

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

        # Filter out rows where a True value is present in columns ending with "_COMPARE"
        if not keep_equal:
            filter_condition = f.expr(
                " OR ".join(f"{col} = False" for col in non_regression_result.columns if col.endswith('_COMPARE')))
            non_regression_result = non_regression_result.filter(filter_condition)

        if not non_regression_result.isEmpty():
            print(
                f"Dataframes are not identical please check the file 'Pipe_3_OUT_mp_application_decay_grouped' for a detailed inspection.")

        return non_regression_result
    else:
        raise ValueError(
            f"DataFrames have different shapes and cannot be compared."
            f"\nShapes -> Df1: {df1.count(), len(df1.columns)} Df2: {df2.count(), len(df2.columns)}"
            f"\nDf1 columns: {df1.columns} Df2 columns: {df2.columns}"
        )


index_cols = [
    'BANK_CONTRACT_CODE', 'KEY_EVENTO', 'OBSERVATION_DATE', 'BANK_CODE', 'REFERENCE_DATE', 'CUST_CODE', 'SERVIZIO',
    'CONTRACT_CODE', 'CONTRACT_CODE_1', 'STATUS', 'DT_EVENTO', 'CURRENCY', 'PRODUCT_TYPE', 'FLAG_RESID',
    'FLAG_MORTGAGE', 'FLAG_WIP', 'JOINT_ACCOUNT_FLAG', 'DISBURSEMENT_DATE',
    # 'DURATION_PREAM',
    # 'ORIGINAL_OUTSTANDING', 'CURRENT_OUTSTANDING', 'PRINCIPAL_INSTALMENT', 'PREPAID_AMOUNT',
    'MATURITY_DATE',
    'MATURITY_RENEGOTIATION_TYPE', 'RATE_RENEGOTIATION_TYPE', 'OTHER_RENEGOTIATION_TYPE', 'AMORTIZATION_TYPE',
    'PRINCIPAL_PAYMENT_FREQUENCY', 'RATE_STRUCTURE_TYPE', 'CURRENT_RATE_TYPE',
    'SPREAD', 'RATE_VALUE',
    'IRO_TYPE',
    # 'FLOOR_VALUE', 'CAP_VALUE',
    'FIN_PHASE_END_DATE',
    'PENALTY',
    # 'LOAN_TO_VALUE',
    'FLAG_CHANGE_BANK',
    'REFERENCE_RATE', 'SECURITIZATION', 'FLAG_PERFORMING', 'SECTOR_CODE', 'CUST_ACTIV_AREA_CODE', 'CUST_TYPE',
    'CUST_TYPE_REGULATORY', 'RATING', 'COD_SEGMENTAZIONE_RATING', 'COD_PROVINCIA_RESIDENZA',
    'FLAG_GARANZIA_PERSONALE', 'FLAG_RAPPORTI_NON_RATEALI', 'FLAG_RAPPORTI_PATRIMONIALI',
    # 'LOAN_TO_VALUE_ORIGINATION',
    'DT_STIPULA',
    # 'CALCOLATA',
    'TR_SAE', 'TR_SAE_SEG',
    'OBSERVATION_DATE_1',
    # 'dtMinRefDate', 'dtMaxRefDate',
    # 'dTimeDelay', 'dTimeAlive',
    # 'iMonthAlive', 'iMonthObs', 'iIndFirstRif',
    # 'iIndLastRif', 'iDurOrig', 'iDurCurrent', 'iTimeDelay',
    # 'RATE_VALUE_OLD', 'EuriborRate', 'SPREAD_INFERRED',
    'DISBURSEMENT_DATE_1',
    # 'TassoParMat0', 'TassoParMatCurr', 'Eur3M0', 'Eur3MCurr', 'Spread0', 'SpreadT',
    # 'dDSpread_IRO', 'dDSpread', 'dRis', 'dRil', 'dRil_RateValue', 'Spread_BTP_BUND', 'Disoccupazione',
    # 'PrezziAbitazioni', 'Inflazione', 'PIL'
]

date_cols = [
    'OBSERVATION_DATE_1',
    'DISBURSEMENT_DATE_1',
    'DT_STIPULA',
    'FIN_PHASE_END_DATE',
    'DT_EVENTO',
    'OBSERVATION_DATE',
]

dfo = "C:/Users/osahenrunmwencole/Desktop/Prometeia/Projects/CCB Prepayment MP/ccb_preproc/data/blob_data_original/preprocessed/PREPAYMENT_CCB_CLEAN_ALL_VAR_NOVEMBER.parquet"  # FULL HISTORICAL BENCHMARK
dfn = "C:/Users/osahenrunmwencole/Desktop/Prometeia/Projects/CCB Prepayment MP/ccb_preproc/tests/mp_data/PreprocWorkdir/PREPAYMENT_CLEAN_MACRO.parquet"  # WITH HISTORICAL DATA
# abs_outp_path = "C:/Users/osahenrunmwencole/Desktop/Prometeia/CodeLibrary/Apache Spark/Pyspark/"

df1 = spark.read.parquet(dfo)
df2 = spark.read.parquet(dfn)

Pipe_3_OUT_mp_application_decay_grouped = assert_dataframes_approx_equal(
    df1=df1, df2=df2, date_columns=date_cols, show_detailed_regression=True,
    # abs_tolerance=0.0001,
    index_columns=index_cols, suffixes=("_SQL", "_SPARK")
)
