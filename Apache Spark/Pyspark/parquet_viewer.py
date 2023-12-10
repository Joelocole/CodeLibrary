import pandas as pd

#df = pd.read_parquet(
#    r"C:\Users\osahenrunmwencole\Desktop\Prometeia\Projects\BPER_Prepayment_to_sparkSQL\local_tests\Pipe_3_OUT_mp_application_decay_grouped.parquet",
#    engine="pyarrow", dtype_backend="numpy_nullable"
#)

df = pd.read_csv(
    r"C:\Users\osahenrunmwencole\Downloads\t_dati_elementari_mens.csv",
    sep=";"
)

df["cut_off"] = pd.to_datetime(df["cut_off"], format='%Y%m%d')
df["cut_off"] = df["cut_off"].dt.strftime('%Y-%m-%d')
#df.to_parquet(engine='pyarrow', path='C:/Users/osahenrunmwencole/Desktop/Prometeia/Projects/BPER_Prepayment_to_sparkSQL/manually_extracted_data/mp_percentiles2.parquet', partition_cols=['strposition'])

print(df["cut_off"].max())