from pymongo import MongoClient

import psycopg2
import re

class MongoDBConnector:
    def __init__(self, database='db', username="mongouser", userpass="123") -> None:
        # Authenticated client
        self.client = MongoClient(f"mongodb://{username}:{userpass}@localhost", 27017)
        self.db_client = self.client[database]
    
    def insert(self, collection='collection', entry={}) -> None:
        self.db_client[collection].insert_one(entry)
        
    def insert_many(self, collection='collection', entries=[]) -> None:
        self.db_client[collection].insert_many(entries)
        
    def read_all(self, collection='collection'):
        return self.db_client[collection].find()
    
    def find(self, collection='collection', query={}, projection={}):
        return self.db_client[collection].find(query, projection=projection)
    
    def unique(self, collection='collection', fields=['field']):
        fields = {
            field:f"${field}" 
            for field in fields
        }
        
        projection = {
            f"{field}":f"$_id.{field}"
            for field in fields
        }
        projection["_id"]=0
        
        return (self.db_client[collection]
                    .aggregate(
                        [
                            {"$project": {"uniques":fields}},
                            {"$group":{"_id":"$uniques"}},
                            {"$project": projection},
                        ]
                    )
                )


class PostgreSQLConnector:
    def __init__(self, 
                 host="localhost", 
                 port="5432", 
                 user="postgres", 
                 password="123",
                 database="postgres"
                 ) -> None:
        """
        PostgreSQL connector.
        Expose some methods to ease the interaction with the database.

        Parameters
        ----------
        host : str, optional
            Postgres host, by default "localhost"
        port : str, optional
            Postgres connection port, by default "5432"
        user : str, optional
            Username, by default "postgres"
        password : str, optional
            Password for username, by default "123"
        database : str, optional
            Database, by default "postgres"
        """
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        """
        Create a connection with Postgres

        Returns
        -------
        pyscopg_connection
        """
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
            
    def execute(self, sql=""):
        """
        Tries to execute a SQL query in a transaction.

        Parameters
        ----------
        sql : str, optional
            String with the SQL query, by default ""
        """
        
        # Transational block
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()    
            cursor.execute(sql)
            conn.commit()
            cursor.close()
        except psycopg2.DatabaseError as ex:
            print(ex)
        finally:
            if conn is not None:
                conn.close()

    def insert(self, collection='collection', entry={}) -> None:
        """
        Recives a entry and stores them into the database

        Parameters
        ----------
        collection : str
            Table/Collection/Schema where to insert the data, by default 'collection'
        entry : dict, optional
            Dictionary representing the record to be inserted
            with format { key:value }
            by default {}
        """        
        
        fields = (
            str(tuple(entry.keys()))
            .replace('\'', '')
            .replace(',)', ')')
        )
        values = (
            str(tuple(entry.values()))
            .replace(',)', ')')
        )
        
        # HARD Processing NULL values
        values = re.sub(r"None", 'NULL', values)
        values = re.sub(r"'NULL'", 'NULL', values)
        
        command = f"""
            INSERT INTO {collection}{fields}
            VALUES {values};
        """
        
        
        self.execute(command)

    def insert_many(self, collection='collection', entries=[]) -> None:
        """
        Recives a list of entries and stores them into the database

        Parameters
        ----------
        collection : str
            Table/Collection/Schema where to insert the data, by default 'collection'
        entry : dict, optional
            Lis of dicts representing the records to be inserted
            with format { key:value }
            by default {}
        """
        
        fields = (
            str(tuple(entries[0].keys()))
            .replace("'", '')
            .replace(',)', ')')
        )
        
        values = [str(tuple(entry.values())) for entry in entries]
        values = (
            ','.join(values)
            .replace(',)', ')')
        )
        
        # HARD Processing NULL values
        values = re.sub(r"None", 'NULL', values)
        values = re.sub(r"'NULL'", 'NULL', values)
        
        command = f"""
            INSERT INTO {collection}{fields}
            VALUES {values};
        """
        
        self.execute(command)

    def find(self, collection='collection', query=""):
        """
        Return a list of objects that match the query.

        Parameters
        ----------
        collection : str
            Table/Collection/Schema where to insert the data, by default 'collection'
        query : str, optional
            Select query, by default ""

        Returns
        -------
        entries
            List of objects
        """        
        entries = []
        
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            
            while row is not None:
                entries.append(row)
                row = cursor.fetchone()
                
            conn.commit()
            cursor.close()
            return entries
        except psycopg2.DatabaseError as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        
        return entries