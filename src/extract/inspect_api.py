import requests
import json

URLS = {
    "station_information": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
    "station_status": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
}

for name, url in URLS.items():
    print("\n" + "=" * 80)
    print(f"API : {name}")
    print("=" * 80)

    response = requests.get(url, timeout=20)
    response.raise_for_status()

    data = response.json()

    print("Clés principales du JSON :")
    print(data.keys())

    print("\nClés dans data :")
    print(data.get("data", {}).keys())

    stations = data.get("data", {}).get("stations", [])

    print(f"\nNombre de stations : {len(stations)}")

    if stations:
        print("\nPremier objet station :")
        print(json.dumps(stations[0], indent=2, ensure_ascii=False))

        print("\nColonnes disponibles :")
        print(list(stations[0].keys()))