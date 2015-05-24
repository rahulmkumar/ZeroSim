import pandas as pd
import sqlite3
import csv
import sys


def main():

    db_filename = 'marketdata.db'
    #con = db.connect(db_filename)

    file_name = sys.argv[1]
    data_filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo/'+file_name+'.csv'
    
    SQL = """
            insert into """+str(file_name)+""" (Date, Open, High, Low, Close, Volume, Adj_Close)
            values (?,?,?,?,?,?,?)
        """
    print SQL
    with open(data_filepath, 'rt') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        to_db = [(i['Date'],i['Open'],i['High'],i['Low'],i['Close'],i['Volume'],i['Adj Close']) for i in csv_reader]
        with sqlite3.connect(db_filename) as conn:
            cursor = conn.cursor()
            cursor.executemany(SQL,to_db)

if __name__ == '__main__':
    main()