import sqlite3
import polars as pl

conn = sqlite3.connect("formulas.db")
cursor = conn.cursor()

df = pl.read_database("SELECT * FROM formulas", conn)
print(df)

conn.commit()
conn.close()
