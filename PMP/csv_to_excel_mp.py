import json
import zipfile
import os
import pandas as pd
import io
from tempfile import NamedTemporaryFile, gettempdir

# Print the version of pandas
print("Pandas version:", pd.__version__)

# creating temp_dir for excel writer
output_excel = os.path.join(gettempdir(),'tmp_convert_excel.xlsx')

if os.path.isfile(output_excel):
  os.remove(output_excel)

# loading json data
json_zipped = get_input('tables_schema_0_0', as_zip=True)

for zipinfo in json_zipped.infolist():
  with json_zipped.open(zipinfo) as f:
    tables_dict = json.load(f)

# loading csv data from zip

csvs_zipped = get_input('csvs_sample_0_0', as_zip=True)

# list of dataframes
dfs = {}

# iterating through zip file in memory
for i, zipinfo in enumerate(csvs_zipped.infolist()):
    table_name = csvs_zipped.namelist()[i]
    print(f"processing file: '{table_name}'.")
    if zipinfo.endswith(".csv"):
        continue


    with csvs_zipped.open(zipinfo) as f:
        first_file = f.read()

        # create buffer obct
        buffer = io.BytesIO(first_file)

        # handling types
        dict_types = {k: v["type"] if v["type"] != "datetime64" else "string" for k, v in tables_dict[table_name.split('.')[0]]["type_map"].items()}
        dates_cols = [k for k, v in tables_dict[table_name.split('.')[0]]["type_map"].items() if v["type"] == "datetime64"]

        dateparse = lambda x: pd.to_datetime(x, format="%Y-%m-%d")
        # read csv
        df = pd.read_csv(
          buffer,
          encoding="utf-8",
          sep=";",
          dtype=dict_types,
          parse_dates=dates_cols,
          date_parser=dateparse
        )

        # handling nullable fields
        non_nullable_cols = [k for k, v in tables_dict[table_name.split('.')[0]]["type_map"].items() if not v["nullable"]]
        for col in non_nullable_cols:
            if df[col].isnull().any():
                raise ValueError(f"Column {col} has null values")

        # conversion dates to string
        for c in dates_cols:
            df[c] = df[c].astype('string')

        dfs[table_name] = df

with pd.ExcelWriter(output_excel) as wrt:
  for table_name, df in dfs.items():
    df.to_excel(wrt, sheet_name=table_name, index=False)


# Writing output:
set_output(output_excel)