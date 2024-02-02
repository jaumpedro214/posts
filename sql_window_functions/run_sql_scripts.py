import sqlite3

PATH_QUERIES = 'queries'
SCRIPT = '2_population_diff_by_country.sql'

con = sqlite3.connect('examples.sqlite')

with open(PATH_QUERIES + '/' + SCRIPT, 'r') as f:
    select_query = f.read()
    cur = con.cursor()
    cur.execute(select_query)
