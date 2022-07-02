import yaml
from yaml.loader import SafeLoader

import yaml
from yaml.loader import SafeLoader

from datetime import datetime
from collections import defaultdict


class DataMartCreator:
    """
    Read a YAML file with the table specifications
    and creates them into the database.
    """
    def __init__(self, db_connector, config_file="file.yaml") -> None:
        self.db_connector = db_connector
        self.config_file = config_file
        
    def read_config_file(self):
        with open(self.config_file) as f:
            data = yaml.load(f, SafeLoader)
        return data
        
    def create_dimension_table(self, name, fields,):
        """
        Creates a dimension table/schema/collection
        in the database.
        The final table will contain the fields specified
        + an numerical id column. 

        Parameters
        ----------
        name : str
            Table name
        fields : list of dicts
            List of python dicts representing the fields to be created
            following the format { field_name: field_type }

        Returns
        -------
        str
            The sql command executed.
        """
        
        linebreaker = ",\n\t\t"
        sql_command = f"""
            CREATE TABLE IF NOT EXISTS {name}(
                id SERIAL PRIMARY KEY,
                {
                    linebreaker.join(
                        {
                            " ".join([*field])
                            for field in fields.items()
                        }
                    )
                }
            );
        """
        
        print(sql_command)
        self.db_connector.execute(sql_command)
        return sql_command
    
    def create_facts_table(self, name, fields, dimensions):
        """
        Creates a facts table/schema/collection
        in the database.
        The final table will contain the fields specified
        + created_on a TIMESTAMP column which stores the moment when a record is created
        + one FOREINGH KEY id column for each dimention table.

        Parameters
        ----------
        name : str
            Table name
        fields : list of dicts
            List of python dicts representing the fields to be created
            following the format { field_name: field_type }
        dimensions : list of dicts
            List of python dicts representing the dimensions table
            following the format { table_name: {...} }

        Returns
        -------
        str
            The sql command executed.
        """
        
        linebreaker = ",\n\t\t"
        sql_command = f"""
            CREATE TABLE IF NOT EXISTS {name}(
                created_on TIMESTAMP NOT NULL DEFAULT(NOW()),
                {
                    linebreaker.join(
                        {
                            " ".join([*field])
                            for field in fields.items()
                        }
                    )
                },
                
                {
                    linebreaker.join([
                        f"{dim_name} INTEGER NOT NULL, FOREIGN KEY({dim_name}) REFERENCES {dim_name}(id)"
                        for dim_name in dimensions.keys()
                    ])
                }
            );
        """
        
        print(sql_command)
        self.db_connector.execute(sql_command)
        return sql_command
    
    def create(self):
        """
        Reads the configuration file and runs the
        appropriate SQL commands to create the tables.
        """
        
        data = self.read_config_file()
        dimensions = data['dimensions']
        facts = data['facts']
        
        for name, fields in dimensions.items():
            self.create_dimension_table(name, fields)
            
        self.create_facts_table(facts['name'], facts['fields'], dimensions)


class DataMartPopulator:
    def __init__(
        self, 
        db_source_connector,
        db_destiny_connector,
        config_file="file.yaml", 
        batch_size=50000,
        transformations=defaultdict(lambda: lambda i:i)
    ) -> None:
        """
        Extract data from a database instance to populate a 
        data mart.
        Populates both dimentions and facts tables.
        
        Parameters
        ----------
        db_source_connector : DBconector - like
            Source database from where to extract the data.
        db_destiny_connector : DBconector - like
            Final database to where populate the Data Mart.
        config_file : str, optional
            YAML file describing the Data Mart Structure, by default "file.yaml"
        batch_size : int, optional
            Number of documents/records/entries to store at once in the destiny database.
            This number is only used to insert new recods in the facs table.
            by default 50000
        transformations : dict, optional
            Dictionary of functions to be applied to each field, by default defaultdict(lambda: lambda i:i)
        """
        
        self.db_source_connector = db_source_connector
        self.db_destiny_connector = db_destiny_connector
        
        self.batch_size = batch_size
        self.config_file = config_file
    
        self.data_mart_config = self.read_config_file()
        self.transformations = transformations
        
    def read_config_file(self):
        with open(self.config_file) as f:
            data = yaml.load(f, SafeLoader)
        return data
    
    def populate_dimension(
        self,
        fields=[],
        from_collection="collection",
        to_collection="collection2"
    ):
        """
        Extract the unique values from fields of a table/collection/schema
        to populate a DataMart Dimension.
        
        Parameters
        ----------
        fields : list, optional
            Fields that compose the unique key, by default []
        from_collection : str, optional
            Original table/collection/schema where the data is stored in the orignal Database, 
            by default "collection"
        to_collection : str, optional
            Dimension table to populate, by default "collection2"
        """
        # Unique Values 
        unique_values = self.db_source_connector.unique(from_collection, fields)
        unique_values = list(unique_values)
        
        # Adding unique id to each entry
        for i, unique_dict in enumerate(unique_values):
            unique_dict["id"] = i
        
        self.db_destiny_connector.insert_many(
            collection=to_collection, 
            entries=unique_values
        )
    
    def populate_dimensions(self, from_collection="collection"):
        """
        Populates all Data Mart dimensions.
        
        [WIP]: Currently, only works when all the data is stored in a
        single table/collection/schema.

        Parameters
        ----------
        from_collection : str, optional
            Original table/collection/schema where the data is stored in the orignal Database, 
            by default "collection"
        """
        
        for table, fields in self.data_mart_config['dimensions'].items():
            self.populate_dimension(
                fields=[*fields.keys()],
                from_collection=from_collection,
                to_collection=table
            )
    
    def get_dimension_ids(self, collection="collection", fields=["id"]):
        """
        Reads the values from a dimension table and creates a
        dict mapping values to ids.
        
        This function essentially loads all the entries from the table
        in memory, and its result can be used to avoid redundant queries to
        the database.

        Parameters
        ----------
        collection : str, optional
            Dimension table/collection/schema, by default "collection"
        fields : list, optional
            Fields(columns) to select from the table. 
            Used to specify the columns' order in the dict key field. 
            by default ["id"]

        Returns
        -------
        dict:
            Dict mapping values (tuple) to ids (int).
        """
        
        # Making sure that the "id" field is always
        # in the first positon, this eases the 
        # final dict creation
        if "id" not in fields:
            fields = ["id"] + fields
        else:
            if fields[0] != "id":
                fields.remove("id")
                fields = ["id"] + fields
        fields = ",".join(fields)
        
        instances = self.db_destiny_connector.find(
            collection,
            query = f"SELECT {fields} FROM {collection};"
        )
        
        # Transforming instances in the key-value pairs
        values_to_id_dict = {
                tuple(instance[1:]):instance[0]
                for instance in instances
        }
        print(values_to_id_dict)
        return values_to_id_dict
            
    def populate_facts(
        self,
        from_collection="collection",
        query={}
    ):
        """
        Populates the Data Mart's facts table with
        records from the source database.

        Parameters
        ----------
        from_collection : str, optional
            Table/collection/schema in the original database from where to extract the data,
            by default "collection"
        query: dict, optional
            Query to be used to filter the data, by default {}
        """
        
        # Importing configurations
        facts_config = self.data_mart_config['facts']
        dimensions_config = self.data_mart_config['dimensions']
        facts_name = facts_config['name']
        facts_fields = [*facts_config['fields'].keys()]
        
        # Defining additional transformations
        # for specific fields
        transform_dict = self.transformations
        
        # Getting dimentions values's ids
        dimention_value_to_id = {
            name: {
                "value_to_id":self.get_dimension_ids(
                    collection=name, 
                    fields=list(fields_config.keys())
                ),
                "fields_order":list(fields_config.keys())
            }
            for name, fields_config in dimensions_config.items()
        }
        
        print(f"[{datetime.now()}] Adding new records to the FACTS table {facts_name}.")
        db_cursor = self.db_source_connector.find(
            collection=from_collection,
            query=query
        )
        
        count = 0
        transformed_documents = []
        print(f"[{datetime.now()}] Inserting facts...")
        
        for document in db_cursor:
            count+=1
            transformed_document=dict()
            
            # Adding facts' fields
            for field in facts_fields:
                transformed_document[field] = transform_dict[field](document[field])
            
            # Adding the dimensions ids (FOREINGH KEYS)
            for dim_name, fields_config in dimensions_config.items():
                values_to_id = dimention_value_to_id[dim_name]
                
                dim_values = tuple(
                    document[field]
                    for field 
                    in values_to_id["fields_order"]
                )
                transformed_document[dim_name] = values_to_id["value_to_id"][dim_values]
            
            transformed_documents.append(transformed_document)
            
            if count % self.batch_size == 0 and count != 0:
                print(f"[{datetime.now()}] {count} processed. Inserting into database.")
                
                self.db_destiny_connector.insert_many(
                    collection=facts_name, 
                    entries=transformed_documents
                )
                
                transformed_documents = []
                count=0
                print(f"[{datetime.now()}] finished.")
        
        
        # Insert the rest of the documents
        # if there are any
        if len(transformed_documents) < self.batch_size:
            print(f"[{datetime.now()}] {count} processed. Inserting into database.")
            self.db_destiny_connector.insert_many(
                collection=facts_name, 
                entries=transformed_documents
            )
            print(f"[{datetime.now()}] finished.")
            
      