# %%
# pip install obspy matplotlib
from obspy.clients.fdsn import RoutingClient, Client
from obspy import UTCDateTime, Stream
import pathlib

# --- PARAMETRI UTENTE ---
LAT, LON = 46.234233, 6.055018   # CERN approx
MAXRADIUS_DEG = 0.01  
start = UTCDateTime("2025-07-29T23:24:00")   # <-- UTC
duration_sec = 180 * 60
end = start + duration_sec
CHANNELS = "HH?,BH?,EH?"

# %%
# --- CLIENT: routing per scoprire le stazioni ---
rc = RoutingClient("eida-routing")

inventory = rc.get_stations(
    latitude=LAT, longitude=LON, maxradius=MAXRADIUS_DEG,
    channel=CHANNELS, level="response"
)
print(f"Trovate {sum(len(net) for net in inventory)} stazioni.")

# Provider FDSN in fallback (i più probabili per l'area Ginevra)
PROVIDERS = ["RESIF", "ETH", "ORFEUS", "INGV", "GFZ", "ODC"]

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
            print(f"Scaricati da {c.base_url}: {net_code}.{sta_code}")
            return wf
        except Exception as e:
            last_err = e
            # prova prossimo provider
    raise last_err if last_err else RuntimeError("Nessun provider disponibile.")

# --- scarico con fallback ---
st = Stream()
for net in inventory:
    for sta in net:
        try:
            wf = try_get_waveforms(net.code, sta.code)
            st += wf
        except Exception as e:
            print(f"Fail {net.code}.{sta.code}: {e}")

if len(st) == 0:
    raise RuntimeError("Nessun dato scaricato: prova ad allargare raggio, cambiare orario o provider.")

# --- pulizia & risposta strumentale ---
st.merge(method=1, fill_value="interpolate")
st.detrend("linear")
st.taper(max_percentage=0.01)

pre_filt = (0.05, 0.08, 40.0, 45.0)  # da adattare al sampling reale
try:
     st.remove_response(inventory=inventory, pre_filt=pre_filt, output="VEL",
                        zero_mean=True, taper=True)
     print("Risposta strumentale rimossa (output=VEL).")
except Exception as e:
     print(f"Rimozione risposta: {e}")

# --- salvataggio ---
out_dir = pathlib.Path("seismo_cern")
out_dir.mkdir(exist_ok=True)
mseed_path = out_dir / f"waveforms_{start.date}T{start.time.strftime('%H%M%S')}_UTC_30min.mseed"
st.write(str(mseed_path), format="MSEED")
print(f"Dati salvati in: {mseed_path}")

# --- grafico rapido ---
try:
    st.plot(size=(1000, 800))
except Exception as e:
    print(f"Nessun backend grafico disponibile: {e}")
# %%
