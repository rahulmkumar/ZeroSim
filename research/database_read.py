import sqlite3
import pandas.io.sql as sql

db_filename = 'C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\marketdata.db'

db_filename = 'marketdata.db'
con = sqlite3.connect(db_filename)

df_aa1 = sql.read_frame('select Date, Close from AA_TBL',con, index_col = 'Date')

