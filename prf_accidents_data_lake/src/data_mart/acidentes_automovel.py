import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from connectors import MongoDBConnector, PostgreSQLConnector

from data_mart import DataMartCreator
from data_mart import DataMartPopulator

from collections import defaultdict

# Script to create and populate the data mart for accidents by vehicle = 'AUTOMOVEL'


if __name__ == "__main__":
    
    # Create the Data Mart
    # Creates dimension tables and fact table
    # using the configuration file
    DataMartCreator(
        db_connector=PostgreSQLConnector(
            user="PRFACIDENTES",
            database="PRFACIDENTES"
        ),
        config_file="./acidentes_automovel.yaml"
    ).create()
    
    # Populate the Data Mart
    
    # The transform dict is responsible for transforming the data from the source database
    # to the data format required by the data mart.
    # If no specific transformation is defined for a field
    # the default is just the identity function.
    transform_dict = defaultdict(lambda: lambda i: i )
    transform_dict['data'] = lambda date: date.strftime("%Y-%m-%d %H:%M:%S")
    
    acidentes_automovel_data_mart = DataMartPopulator(
        db_source_connector=MongoDBConnector(
            database="PRFAcidentes"
        ),
        db_destiny_connector=PostgreSQLConnector(
            user="PRFACIDENTES",
            database="PRFACIDENTES"
        ),
        config_file="./acidentes_automovel.yaml",
        batch_size=50000,
        transformations=transform_dict
    )
    
    
    # Populating dimensions
    # The dimensions are specified in the config file
    acidentes_automovel_data_mart.populate_dimensions(
        from_collection="processed"
    )
    
    # Populating facts
    # Here, the query is specific for the data mart
    # The query is based on the data in the collection "processed" (MongoDB)
    # It filters by the vehicle type "AUTOMOVEL" 
    acidentes_automovel_data_mart.populate_facts(
        from_collection="processed",
        query={"tipo_veiculo":"automóvel"}
    )