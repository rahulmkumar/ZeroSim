import pandas as pd
import sqlite3 as db
import csv
import sys


def main():

    db_filename = 'marketdata.db'
    #con = db.connect(db_filename)

    file_name = sys.argv[1]
    data_filepath = "C:\Python27\Lib\site-packages\QSTK\QSData\Yahoo\\"+file_name+".csv"
    print data_filepath

    SQL = """
    insert into """+str(file_name)+"""(Date, Open, High, Low,Close, Volume, Adj_Close) values (:Date, :Open, :High, :Low, :Close, :Volume, :Adj_Close)
    """
    
    print SQL

if __name__ == '__main__':
    main()    