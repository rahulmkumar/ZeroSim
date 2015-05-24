import pandas as pd
import sqlite3
import sys
import csv
import platform


def main():
    symbol_file = sys.argv[1]
    
    sym_list = create_sqlite_tables(symbol_file)
    load_sqlite_data(sym_list)

def load_sqlite_data(sym_list):
    db_filename = 'marketdata.db'
    
    for sym in sym_list:
        if platform.system() == 'Linux':
            data_filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo/'+sym+'.csv'
        elif platform.system() == 'Windows': 
            data_filepath = 'C:/Python27/Lib/site-packages/QSTK/QSData/Yahoo/'+sym+'.csv'
        
        insert_SQL = """
                insert into """+str(sym)+"""_TBL (Date, Open, High, Low, Close, Volume, Adj_Close)
                values (?,?,?,?,?,?,?)
            """
        delete_SQL = """delete from """+str(sym)+"""_TBL"""
        #print delete_SQL
        #print SQL
        try:
            with open(data_filepath, 'rt') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                to_db = [(i['Date'],i['Open'],i['High'],i['Low'],i['Close'],i['Volume'],i['Adj Close']) for i in csv_reader]
                with sqlite3.connect(db_filename) as conn:
                    cursor = conn.cursor()
                    cursor.execute(delete_SQL)
                    cursor.executemany(insert_SQL,to_db)
            conn.commit()
            #conn.close()
        except IOError:
            #print 'Could not find '+sym+'.csv'
            print 'count not find '+data_filepath
    conn.close()

def create_sqlite_tables(symbol_file):
    
    db_filename = 'marketdata.db'
    
    if platform.system() == 'Linux':
        sym_filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo/Lists/'
    elif platform.system() == 'Windows':
        sym_filepath = 'C:/Python27/Lib/site-packages/QSTK/QSData/Yahoo/Lists/'
    
    symbols = [line.strip() for line in open(sym_filepath+symbol_file+".txt",'r')]

    con = sqlite3.connect(db_filename)
    cursor = con.cursor()
    
    for sym in symbols:
        cursor.execute("""select count(*) from sqlite_master where type = 'table' and tbl_name = '"""+str(sym)+"""_TBL'""")
        for row in cursor.fetchall():
            table_count = row
        if table_count == (0,):
            create_table_query = """ CREATE TABLE """ +str(sym)+"""_TBL (Date DATE, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER, Adj_Close REAL);"""
            con.execute(create_table_query)
    con.commit()
    con.close()
    
    return symbols
        

if __name__ == '__main__':
    main()    