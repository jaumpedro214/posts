# SOURCE - https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes
import os
import sys
import gdown

links = {
    2022:"https://drive.google.com/uc?id=1PRQjuV5gOn_nn6UNvaJyVURDIfbSAK4-",
    2021:"https://drive.google.com/uc?id=12xH8LX9aN2gObR766YN3cMcuycwyCJDz",
    2020:"https://drive.google.com/uc?id=1esu6IiH5TVTxFoedv6DBGDd01Gvi8785",
    2019:"https://drive.google.com/uc?id=1pN3fn2wY34GH6cY-gKfbxRJJBFE0lb_l",
    2018:"https://drive.google.com/uc?id=1cM4IgGMIiR-u4gBIH5IEe3DcvBvUzedi",
    2017:"https://drive.google.com/uc?id=1HPLWt5f_l4RIX3tKjI4tUXyZOev52W0N",
    2016:"https://drive.google.com/uc?id=16qooQl_ySoW61CrtsBbreBVNPYlEkoYm",
    2015:"https://drive.google.com/uc?id=1DyqR5FFcwGsamSag-fGm13feQt0Y-3Da",
    2014:"https://drive.google.com/uc?id=1FpF5wTBsRDkEhLm3z2g8XDiXr9SO9Uk8",
    2013:"https://drive.google.com/uc?id=1p_7lw9RzkINfscYAZSmc-Z9Ci4ZPJyEr",
    2012:"https://drive.google.com/uc?id=18Yz2prqKSLthrMmW-73vrOiDmKTCL6xE",
    2011:"https://drive.google.com/uc?id=1HHhgLF-kSR6Gde2qOaTXL3T5ieD33hpG",
    2010:"https://drive.google.com/uc?id=1_yU6FRh8M7USjiChQwyF20NtY48GTmEX",
    2009:"https://drive.google.com/uc?id=1qkVatg0pC_zosuBs0NCSgEXDJvBbnTYC",
    2008:"https://drive.google.com/uc?id=1_OSeHlyKJw8cIhMS_JzSg1RlYX8k6vSG",
    2007:"https://drive.google.com/uc?id=1EFpZF5F6cB0DOHd2Uxnj7X948WE69a8e"
}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: download_file.py <year>")
        sys.exit(-1)

    year = int(sys.argv[1])

    if year not in links:
        print("Year not found")
        sys.exit(-1)
    
    gdown.download(links[year], f"./../data/acidentes_{year}.zip", quiet=False)

    os.system(f"unzip ./../data/acidentes_{year}.zip -d ./../data/acidentes")
    os.system(f"rm ./../data/acidentes_{year}.zip")
