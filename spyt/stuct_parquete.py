import pandas as pd

data = pd.read_parquet(r"C:\Users\B-ZONE\Downloads\archive\samples\part-0067.parquet")
print(data.info())


