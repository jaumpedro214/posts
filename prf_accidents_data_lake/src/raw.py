import pandas as pd

from collections import defaultdict
from connectors import MongoDBConnector

import argparse
import os

# CONSTANTS & CONFIGURATIONS
COLUMNS = [ 'id', 'pesid', 'data_inversa',
            'dia_semana', 'horario', 'uf', 'br', 'km',
            'municipio', 'causa_acidente', 'tipo_acidente',
            'classificacao_acidente', 'fase_dia',
            'sentido_via', 'condicao_metereologica',
            'tipo_pista', 'tracado_via', 'uso_solo',
            'id_veiculo', 'tipo_veiculo', 'marca', 
            'tipo_envolvido', 'estado_fisico', 'idade','sexo',
          ]
CONFIG = defaultdict(lambda: {"delimiter":";", "encoding":"ISO-8859-1"})
CONFIG[2015] = {"delimiter":",", "encoding":"ISO-8859-1"}
CONFIG[2020] = {"delimiter":";", "encoding":"UTF-8"}

class DataCSVExtractor:
    def __init__(self, columns=None) -> None:
        """
        Read a .csv file and transform it into a list of dicts

        Parameters
        ----------
        columns : list of str, optional
            Subset of columns to be read from the CSV file, by default None
        """
        self.columns = columns
    
    def extract(self, filename="", **kwargs):
        """
        Read a .csv file and transform it into a list of dicts.
        It uses pandas .read_csv method to read the records, 
        and aditional parameters can be passed via **kwargs

        Parameters
        ----------
        filename : str, optional
            CSV filename, by default ""

        Returns
        -------
        list of dicts
            CSV transformed into a list of dicts, where each field represents
            a csv column
        """
        db = pd.read_csv(filename,
                         usecols=self.columns,
                         **kwargs
                        )
        records = db.to_dict(orient="records")
        return records


class PRFDataExtractor:
    
    def __init__(self):
        self.extractor = DataCSVExtractor(columns=COLUMNS)
        self.db_connector = MongoDBConnector(database="PRFAcidentes")
    
    def extract(self, year:int) -> list[dict]:
        """
        Extract the records from a year and stores into a database.
        The records file must be in the path ./data/acidentes{year}.csv

        Parameters
        ----------
        year : int
            The year from which the data will be extracted

        Returns
        -------
        list of dicts
            Records list
        """
        filename = os.path.dirname(__file__)+f"/data/acidentes{year}.csv"
        return self.extractor.extract(filename, **CONFIG[year])
    
    def save(self, records:list[dict]):
        self.db_connector.insert_many(collection="raw", entries=records)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read annual data.')
    parser.add_argument('year', type=int, help='year')
    args = parser.parse_args()
    year = args.year
    
    prf_data_extractor = PRFDataExtractor()
    try:
        records = prf_data_extractor.extract(year)
        print(f"Inserting {len(records)} records.")
        prf_data_extractor.save(records)
    except FileNotFoundError as ex:
        print(ex)
        exit(1)