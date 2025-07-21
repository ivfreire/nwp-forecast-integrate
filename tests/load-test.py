import time
import requests
from concurrent.futures import ThreadPoolExecutor
import json

names = [
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250718T00.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250719T03.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250720T06.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250721T09.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250722T12.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250723T15.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250724T18.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250725T21.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250727T00.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250728T03.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250729T06.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250730T09.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250731T12.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250801T15.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250802T18.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250803T21.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250805T00.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250806T03.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250807T06.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250808T09.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250809T12.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250810T15.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250811T18.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250812T21.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250814T00.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250815T03.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250816T06.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250817T09.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250818T12.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250819T15.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250820T18.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250821T21.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250823T00.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250824T03.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250825T06.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250826T09.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250827T12.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250828T15.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250829T18.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250830T21.nc",
  "downloads/TOK30d1/20250718T00/ws100m-inst_TOK30d1_bra_20250718T00_20250901T00.nc",
] * 100

def make_request(name):
    payload = {
        'name': name,
        'bucket': 'tok_downloader',
        'sink': 'none'
    }
    
    try:
        now = time.time()

        response = requests.post('http://192.168.49.2:30001/process', json=payload)

        elapsed = time.time() - now
        return f"{name}: {response.text} (Elapsed: {elapsed:.2f}s)"

    except Exception as e:
        return f"{name}: Error - {e}"

if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(make_request, names))
        
    print('Results:')

    for result in results:
        print(result)