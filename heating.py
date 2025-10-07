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
# load data
import pandas as pd
filename = 'LHC_data_jul29_to_aug1_2025'

df = pd.read_parquet(f'/eos/project/l/lumimd/LHC_heating/{filename}.parquet')

# convert the index back to datetime utc
df.index = pd.to_datetime(df.index, unit='ns', utc=True)
df.index = df.index.tz_convert('CET')
df.head(10)
# %%
from matplotlib import pyplot as plt
# plot the vacuum gauge data between 30 July and 2 August
df['HX:FILLN'].fillna(method='ffill', inplace=True)
my_filter = (df['HX:FILLN']==10888)
plt.figure(figsize=(10, 6))
plt.plot(df[my_filter]['VGI.79.6L2.B.PR'].dropna())

# %%
seismo_df = pd.read_parquet('./seismo_cern/waveforms_2025-07-29T232400_UTC_30min.parquet')
# %%
seismo_df.index = seismo_df.timestamp
seismo_df.index = pd.to_datetime(seismo_df.index, unit='ns', utc=True)
seismo_df.index = seismo_df.index.tz_convert('CET')
# subsample to 1Hz

# %%
plt.figure(figsize=(10, 6))
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(8, 6))


ax1.plot(seismo_df[seismo_df['channel']=='HH2']['value'].resample('1S').mean()/1000,'k')
ax1.legend(['Seismic sensor in Point 1 (UL16)\n HH2 channel [mm/s]'], loc='upper right')
ax1.axvline(pd.Timestamp('2025-07-30 02:17:10+02:00'), color='red', linestyle='--', label='Start of fill 10888')

# link horizontal axis to the seismo plot



ax2.plot(df[seismo_df.index[0]:seismo_df.index[-1]][ 'LHC.BCTDC.A6R4.B1:BEAM_INTENSITY'].dropna()/1e14,'b',
         label = 'Beam 1 Intensity [1e14 p]')
ax2.legend(loc='upper right')
ax2.axvline(pd.Timestamp('2025-07-30 02:17:10+02:00'), color='red', linestyle='--', label='Start of fill 10888')


ax3.semilogy(df[seismo_df.index[0]:seismo_df.index[-1]]['VGI.79.6L2.B.PR'].dropna(), 'r',
         label = 'VGI.79.6L2.B.PR [mbar]')
ax3.legend(loc='upper right')
ax3.set_xlabel('30 July 2025, time (CET)')
#ax3.axvline(pd.Timestamp('2025-07-30 02:17:10+02:00'), color='red', linestyle='--', label='Start of fill 10888')


#ax3.set_xlim(seismo_df.index[400000], seismo_df.index[1000000])
plt.savefig('seismo_beam_vacuum.png', dpi=300)
# %%
plt.figure(figsize=(10, 6))
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(8, 6))


ax1.plot(seismo_df[seismo_df['channel']=='HH2']['value'].resample('1S').mean()/1000,'k')
ax1.legend(['Seismic sensor in Point 1 (UL16)\n HH2 channel [mm/s]'], loc='upper right')
ax1.axvline(pd.Timestamp('2025-07-30 02:17:10+02:00'), color='red', linestyle='--', label='Start of fill 10888')

# link horizontal axis to the seismo plot



ax2.plot(df[seismo_df.index[0]:seismo_df.index[-1]][ 'LHC.BCTDC.A6R4.B1:BEAM_INTENSITY'].dropna()/1e14,'b',
         label = 'Beam 1 Intensity [1e14 p]')
ax2.legend(loc='upper right')
ax2.axvline(pd.Timestamp('2025-07-30 02:17:10+02:00'), color='red', linestyle='--', label='Start of fill 10888')


ax3.semilogy(df[seismo_df.index[0]:seismo_df.index[-1]]['VGI.79.6L2.B.PR'].dropna(), 'r',
         label = 'VGI.79.6L2.B.PR [mbar]')
ax3.legend(loc='upper right')
ax3.set_xlabel('30 July 2025, time (CET)')


ax3.set_xlim(seismo_df.index[700000], seismo_df.index[900000])
plt.savefig('seismo_beam_vacuum_zoom.png', dpi=300)
# %%
