import os
import requests

BASE_URL = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{}.zip"
YEARS = range(2011, 2021+1)
SAVE_PATH = os.path.join(os.path.dirname(__file__), "data")
UNZIP = True

if __name__ == "__main__":
    for year in YEARS:
        print("Downloading data for year {}".format(year))
        url = BASE_URL.format(year)
        r = requests.get(url, allow_redirects=True, verify=False)
        open(os.path.join(SAVE_PATH, "{}.zip".format(year)), "wb").write(r.content)

        # Unzip the file
        if UNZIP:
            print("Unzipping file")
            os.system(f"unzip {SAVE_PATH}/{year}.zip -d {SAVE_PATH}/{year}")

            csvs_path = f"{SAVE_PATH}/{year}/*/dados/*.CSV"
            os.system(f"mv {csvs_path} {SAVE_PATH}")

            # Delete the zip file
            os.system(f"rm {SAVE_PATH}/{year}.zip")
            # Delete the unzipped folder
            os.system(f"rm -rf {SAVE_PATH}/{year}")

