# %%
#!/usr/bin/env python3
"""
Script to download LHC data from September 1st to 3rd, 2025
Based on the NXCALS example in README.md
"""

import nx2pd as nx
import numpy as np
import pandas as pd
from nxcals.spark_session_builder import get_or_create, Flavor

# Initialize Spark session
spark = get_or_create(
    flavor=Flavor.LOCAL,
    conf={'spark.driver.maxResultSize': '8g',
          'spark.driver.memory': '16g',
          }
)
sk = nx.SparkIt(spark)

# %%
# Define time range: September 1st to 3rd, 2025
t0 = pd.Timestamp('2025-07-29 00:00', tz="CET")
t1 = pd.Timestamp('2025-08-01 23:59', tz="CET")

print(f"Downloading LHC data from {t0} to {t1}")

# Download the requested data
df = sk.get(t0, t1, [
    'LHC.BCTDC.A6R4.B1:BEAM_INTENSITY',  # LHC beam 1 intensity
    'LHC.BCTDC.A6R4.B2:BEAM_INTENSITY',  # LHC beam 2 intensity
    'VGI.77.6L2.R.PR',                   # Vacuum gauge
    'VGI.77.6R8.B.PR',                   # Vacuum gauge
    'VGI.79.6L2.B.PR',                   # Vacuum gauge
    'VGI.79.6R8.R.PR',                   # Vacuum gauge
    'VITCE.DUMMY.6L2.3.TEMPERATURE',     # Temperature sensor3
    'VITCE.DUMMY.6L2.4.TEMPERATURE',     # Temperature sensor4
    'VITCE.DUMMY.6L2.5.TEMPERATURE',     # Temperature sensor5
    'VITCE.DUMMY.6L2.6.TEMPERATURE',     # Temperature sensor6
    'HX:FILLN',                          # Fill number
    'HX:BMODE',                          # Beam mode
])

# Process the data
# for column in df.columns:
#     try:
#         df[column] = df[column].dropna().apply(lambda x: x['elements'])
#     except KeyError:
#         pass

# Convert index to int64 timestamp
df.index = df.index.astype("datetime64[ns]").astype("int64")

# Save to parquet file
filename = 'LHC_data_jul29_to_aug1_2025'
df.to_parquet(f'/eos/project/l/lumimd/LHC_heating/{filename}.parquet')

print(f"Data saved to {filename}.parquet")
print(f"Data shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
# %%
