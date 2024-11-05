import sqlite3

conn = sqlite3.connect("data/db/formulas.db")
cursor = conn.cursor()

init = open("data/db/formulas.sql").read()
for table_init in init.split(";"):
    cursor.execute(table_init)

conn.commit()
conn.close()
