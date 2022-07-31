import os

YEARS = [str(i) for i in range(2010, 2021+1)]
URLS = [
    f"https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip"
    for year in YEARS
]

if __name__ == "__main__":
    
    for year, url in zip(YEARS, URLS):
        command  = f"wget {url} -P ./data"
        command += " && unzip ./data/{} -d ./data".format(url.split("/")[-1])
        command += f" && mv $(echo $(find ./data/{year}/dados/ -regextype posix-extended -regex '.+(csv|CSV)' )) ./data/{year}.csv"
        command += f" && rm -rf ./data/{year}/"
        
        os.system(command)
    os.system("rm -rf ./data/*.zip")
        