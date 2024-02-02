import sqlite3

PATH_QUERIES = 'queries'
SCRIPT = '4_cumulative_salary_by_worker.sql'

con = sqlite3.connect('examples.sqlite')

with open(PATH_QUERIES + '/' + SCRIPT, 'r') as f:
    select_query = f.read()
    cur = con.cursor()
    cur.execute(select_query)
