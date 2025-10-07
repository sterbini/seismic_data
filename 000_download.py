# %%
# pip install obspy matplotlib
from obspy.clients.fdsn import RoutingClient, Client
from obspy import UTCDateTime, Stream
import pathlib

# --- USER PARAMETERS ---
LAT, LON = 46.234233, 6.055018   # CERN approx
MAXRADIUS_DEG = 0.01  
start = UTCDateTime("2025-07-29T23:24:00")   # <-- UTC
#start = UTCDateTime("2025-07-30T00:10:00")   # <-- UTC

duration_sec = 180 * 60
#duration_sec = 3 * 60

end = start + duration_sec
CHANNELS = "HH?,BH?,EH?"

# %%
# --- CLIENT: routing to discover stations ---
rc = RoutingClient("eida-routing")

inventory = rc.get_stations(
    latitude=LAT, longitude=LON, maxradius=MAXRADIUS_DEG,
    channel=CHANNELS, level="response"
)
print(f"Found {sum(len(net) for net in inventory)} stations.")

# FDSN providers fallback (the most likely for the Geneva area)
PROVIDERS = [#"RESIF", 
             "ETH",
             #"ORFEUS", 
             #"INGV",
             #"GFZ",
             #"ODC",
             ]   

clients = []
for prov in PROVIDERS:
    try:
        clients.append(Client(prov, timeout=60))
        print(f"OK provider: {prov}")
    except Exception as e:
        print(f"Skip provider {prov}: {e}")

def try_get_waveforms(net_code, sta_code):
    last_err = None
    for c in clients:
        try:
            wf = c.get_waveforms(
                network=net_code, station=sta_code, location="*",
                channel=CHANNELS, starttime=start, endtime=end
            )
            print(f"Downloaded from {c.base_url}: {net_code}.{sta_code}")
            return wf
        except Exception as e:
            last_err = e
            # try next provider
    raise last_err if last_err else RuntimeError("No provider available.")

# --- download with fallback ---
st = Stream()
for net in inventory:
    for sta in net:
        try:
            wf = try_get_waveforms(net.code, sta.code)
            st += wf
        except Exception as e:
            print(f"Fail {net.code}.{sta.code}: {e}")

if len(st) == 0:
    raise RuntimeError("No data downloaded: try enlarging radius, changing time or provider.")

# --- cleaning & instrument response ---
#st.merge(method=1, fill_value="interpolate")
#st.detrend("linear")
#st.taper(max_percentage=0.01)

pre_filt = (0.01, 0.02, 40.0, 45.0)  # adapt to actual sampling
try:
     st.remove_response(inventory=inventory, 
                        pre_filt=pre_filt,
                        output="VEL",  # VEL, DISPL
                        #zero_mean=True,
                        #taper=True
                        )
     print("Instrument response removed (output=VEL).")
except Exception as e:
     print(f"Response removal: {e}")

# --- saving ---
out_dir = pathlib.Path("seismo_cern")
out_dir.mkdir(exist_ok=True)
mseed_path = out_dir / f"waveforms_{start.date}T{start.time.strftime('%H%M%S')}_UTC_30min.mseed"
st.write(str(mseed_path), format="MSEED")
print(f"Data saved to: {mseed_path}")


# Converti in micrometri
for tr in st:
     tr.data *= 1e6
#     print(tr.id, tr.data[:10], " [µm]")

# --- quick plot ---
try:
    st.plot(size=(1000, 800))
except Exception as e:
    print(f"No graphical backend available: {e}")

# %%
from matplotlib import pyplot as plt
for tr in [st[1], st[0],st[2]]:  # reverse order
     plt.plot(tr.times(), tr.data)
     
plt.title('CERN Seismic Network 2025-07-29 23h24 UTC (+180 min), P1 UL16')
plt.xlabel('Time [s]')
plt.ylabel('Velocity [µm/s]')
plt.legend([tr.id for tr in [st[1], st[0],st[2]]])


# # %%
# velocity= st[2].data
# time= st[2].times()
# # plot velocity
# plt.figure()
# plt.plot(time, velocity)
# plt.title('Velocity')
# plt.xlabel('Time [s]')
# plt.ylabel('Velocity [µm/s]')
# plt.grid()
# #plt.plot(time,200*np.cos(0.2*time))  # example of a sine wave
# # compute of displacement by integrating velocity
# # %%
# import numpy as np
# displacement = np.cumsum(velocity) * (time[1] - time[0
# ])  # simple cumulative sum integration
# # plot displacement
# plt.figure()
# plt.plot(time, displacement)
# plt.title('Displacement from Velocity')
# plt.xlabel('Time [s]')
# plt.ylabel('Displacement [µm]')
# plt.grid()
# #plt.plot(time,200/0.2*np.sin(0.2*time))  # example of a sine wave

# %%
import pandas as pd
def trace_to_dataframe(tr):
    """
    Convert an ObsPy Trace to a tidy pandas DataFrame with UTC timestamps.
    """
    sr = float(tr.stats.sampling_rate)
    n = int(tr.stats.npts)
    # Start time as timezone-aware UTC
    t0 = pd.Timestamp(tr.stats.starttime.datetime, tz="UTC")
    # Offsets in seconds -> Timedelta
    offsets = pd.to_timedelta(np.arange(n) / sr, unit="s")
    ts = t0 + offsets

    df = pd.DataFrame({
        "timestamp": ts,                # UTC
        "value": tr.data.astype(float), # µm/s given your scaling
        "network": tr.stats.network,
        "station": tr.stats.station,
        "location": tr.stats.location,
        "channel": tr.stats.channel,
        "sampling_rate_hz": sr
    })
    return df

import pandas as pd
import numpy as np
df = pd.concat([trace_to_dataframe(tr) for tr in st], ignore_index=True)
# %%
df.to_parquet(out_dir / f"waveforms_{start.date}T{start.time.strftime('%H%M%S')}_UTC_30min.parquet")

# %%
df = pd.read_parquet(out_dir / f"waveforms_{start.date}T{start.time.strftime('%H%M%S')}_UTC_30min.parquet")
from matplotlib import pyplot as plt
# %%
aux = df[df['channel']=='HH3']
plt.plot(aux['timestamp'], aux['value'])
# %%
