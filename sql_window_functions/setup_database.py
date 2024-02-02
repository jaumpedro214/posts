import sqlite3
import os

CREATE_DATABASE_FILE = 'create_database.sql'

# drop the database if it exists
try:
    os.remove('examples.sqlite')
except FileNotFoundError:
    pass

con = sqlite3.connect('examples.sqlite')
# Execute script to create database
with open(CREATE_DATABASE_FILE, 'r') as f:
    create_database_sql = f.read()
    con.executescript(create_database_sql)
    
    # Commit the changes

con.close()