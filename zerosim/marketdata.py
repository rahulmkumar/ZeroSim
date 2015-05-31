import QSTK.qstkutil.qsdateutil as du
import sqlite3
import datetime as dt
import QSTK.qstkutil.DataAccess as da
from datetime import datetime
import pandas as pd
import sys
import platform
import pandas.io.sql as sql
import urllib2
import urllib
import os
import time
import csv
import socket

class MyException(Exception):
    pass

def file_path(item):
    if item == 'symbol':
        if platform.system() == 'Linux':
            filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo/Lists/'
        elif platform.system() == 'Windows':
            filepath = 'C:\Python27\Lib\site-packages\QSTK\QSData\Yahoo\Lists\\'

    if item == 'data':        
        if platform.system() =='Linux':
            filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo//'
        elif platform.system()=='Windows':
            filepath = 'C:/Python27/Lib/site-packages/QSTK/QSData/Yahoo/'
    if item == 'db':
        if platform.system()=='Linux':
            filepath = '/home/rahul/workspace/ZSSuite/marketdata.db'
        elif platform.system() == 'Windows':
            filepath = 'C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\marketdata.db'
    
    return filepath


def get_yahoo_data(data_path, ls_symbols):
    '''Read data from Yahoo
    @data_path : string for where to place the output files
    @ls_symbols: list of symbols to read from yahoo
    '''
    # Create path if it doesn't exist
    if not (os.access(data_path, os.F_OK)):
        os.makedirs(data_path)

    ls_missed_syms = []
    # utils.clean_paths(data_path)   

    _now = dt.datetime.now()
    # Counts how many symbols we could not get
    miss_ctr = 0
    for symbol in ls_symbols:
        # Preserve original symbol since it might
        # get manipulated if it starts with a "$"
        symbol_name = symbol
        if symbol[0] == '$':
            symbol = '^' + symbol[1:]

        symbol_data = list()
        # print "Getting {0}".format(symbol)

        try:
            params = urllib.urlencode ({'a':0, 'b':1, 'c':2000, 'd':_now.month-1, 'e':_now.day, 'f':_now.year, 's': symbol})
            url = "http://ichart.finance.yahoo.com/table.csv?%s" % params
            #try:
            url_get = urllib2.urlopen(url, timeout=5)
            #except socket.timeout, e:
            #raise MyException("URL Timeout: %r" % e)
                
            
            header = url_get.readline()
            symbol_data.append (url_get.readline())
            while (len(symbol_data[-1]) > 0):
                symbol_data.append(url_get.readline())

            # The last element is going to be the string of length zero. 
            # We don't want to write that to file.
            symbol_data.pop(-1)
            #now writing data to file
            f = open (data_path + symbol_name + ".csv", 'w')

            #Writing the header
            f.write (header)

            while (len(symbol_data) > 0):
                f.write (symbol_data.pop(0))

            f.close()

        except urllib2.HTTPError:
            miss_ctr += 1
            ls_missed_syms.append(symbol_name)
            print "Unable to fetch data for stock: {0} at {1}".format(symbol_name, url)
        except urllib2.URLError:
            miss_ctr += 1
            ls_missed_syms.append(symbol_name)
            print "URL Error for stock: {0} at {1}".format(symbol_name, url)
        except socket.timeout, e:
            miss_ctr += 1
            ls_missed_syms.append(symbol_name)
            print "URL Timeout for stock: {0} at {1}".format(symbol_name, url)
            print e

    print "All done. Got {0} stocks. Could not get {1}".format(len(ls_symbols) - miss_ctr, miss_ctr)
    return ls_missed_syms


def read_symbols(s_symbols_file):
    '''Read a list of symbols'''
    ls_symbols = []
    ffile = open(s_symbols_file, 'r')
    for line in ffile.readlines():
        str_line = str(line)
        if str_line.strip(): 
            ls_symbols.append(str_line.strip())
    ffile.close()
    return ls_symbols 


def update_my_data():
    '''Update the data in the root dir'''
    c_dataobj = da.DataAccess('Yahoo', verbose=True)
    s_path = c_dataobj.rootdir
    ls_symbols = c_dataobj.get_all_symbols()
    ls_missed_syms = get_yahoo_data(s_path, ls_symbols)
    # Making a second call for symbols that failed to double check
    get_yahoo_data(s_path, ls_missed_syms)
    return

def update_yahoo_files(file_name):

    sym_filepath = file_path('symbol')
    data_filepath = file_path('data')

    symbol_path = sym_filepath + file_name + '.txt'
    #ls_symbols = read_symbols('/home/rahul/QSTK-0.2.6/Examples/Features/symbols.txt')
    ls_symbols = read_symbols(symbol_path)
    get_yahoo_data(data_filepath, ls_symbols)


def read_finviz_US(file_name):
    
    path = './symbols/'+file_name+'_Finviz.csv'
    df_finviz = pd.read_csv(path,index_col=['Ticker'])
    return df_finviz

def read_quandl_US(file_name):
    #www.quandl.com/help/api/resources
    
    path = './symbols/'+file_name+'_Quandl.csv'
    df_quandl = pd.read_csv(path,index_col=['Ticker'])
    return df_quandl

def combine_tickers_US(file_name):
    
    df_finviz = read_finviz_US(file_name)
    df_quandl = read_quandl_US(file_name)
    
    df_exch = pd.merge(df_finviz,df_quandl, left_index=True, right_index=True)
    
    #df_exch = df_quandl
    
    #Create csv file of combined symbols
    path = './symbols/'+file_name+'.csv'
    df_exch.to_csv(path)
    
    #Update table in the database with csv fields
    db_filename = file_path('db')
    
    con = sqlite3.connect(db_filename)
    cursor = con.cursor()
    
    # See if the table already exists, if not, then create it
    cursor.execute("""select count(*) from sqlite_master where type = 'table' and tbl_name = '"""+str(file_name)+"""_SYM_TBL'""")

    for row in cursor.fetchall():
        table_count = row

    if table_count == (0,):
        create_table_query = """ CREATE TABLE """ +str(file_name)+"""_SYM_TBL (Ticker VARCHAR(6), Company VARCHAR(50), Sector VARCHAR(30), Industry VARCHAR(50), Country VARCHAR(10));"""
        cursor.execute(create_table_query)
        con.commit()

    # Insert the csv file data into the table
    data_filepath = './symbols/'+file_name+'.csv'
    insert_SQL = """ insert into """+str(file_name)+"""_SYM_TBL (Ticker, Company, Sector, Industry, Country) values (?,?,?,?,?)	"""
    delete_SQL = """delete from """+str(file_name)+"""_SYM_TBL"""
    #print delete_SQL
    #print SQL
    try:
        with open(data_filepath, 'rt') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            to_db = [(i['Ticker'],i['Company'],i['Sector'],i['Industry'],i['Country']) for i in csv_reader]
            with sqlite3.connect(db_filename) as conn:
                cursor = conn.cursor()
                cursor.execute(delete_SQL)
                cursor.executemany(insert_SQL,to_db)
        conn.commit()
        #conn.close()
    except IOError:
        #print 'Could not find '+sym+'.csv'
        print 'count not find '+data_filepath

    con.close()
    
    #Create text file of just the symbols
    sym_filepath = file_path('symbol')
    path = sym_filepath+file_name+".txt"
    f = f = open(path,"w")
    for sym in df_exch.index:
        f.write(sym+"\n")

def load_sqlite_data(sym_list):
    #db_filename = 'marketdata.db'
    db_filename = file_path('db')
    
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
    '''
	if platform.system() == 'Linux':
		sym_filepath = '/usr/local/lib/python2.7/dist-packages/QSTK-0.2.6-py2.7.egg/QSTK/QSData/Yahoo/Lists/'
	elif platform.system() == 'Windows':
		sym_filepath = 'C:/Python27/Lib/site-packages/QSTK/QSData/Yahoo/Lists/'
	'''
    
    #db_filename = 'marketdata.db'
    db_filename = file_path('db')
    sym_filepath = file_path('symbol')

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

def get_sqlitedb_data_all(startdate, enddate, symbol_file, benchmark):

    print 'This is the sqlite database function'
    
    sym_filepath = file_path('symbol')
    
    ls_symbols = [line.strip() for line in open(sym_filepath+symbol_file+".txt",'r')]
    if benchmark not in ls_symbols:
        ls_symbols.append(benchmark)
    columns = ls_symbols
   

    start_month = startdate.split('/')[0]
    start_day = startdate.split('/')[1]
    start_year = startdate.split('/')[2]

    
    
    end_month = enddate.split('/')[0]
    end_day = enddate.split('/')[1]
    end_year = enddate.split('/')[2]


    dt_start = dt.datetime(int(start_year), int(start_month), int(start_day))
    dt_end = dt.datetime(int(end_year), int(end_month), int(end_day))

    dt_timeofday = dt.timedelta(hours=16)
    

    # Get a list of trading days between the start and the end.
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
    
    #print ldt_timestamps

    df_price_asc = pd.DataFrame(index=ldt_timestamps,columns = columns)
    df_close = df_price_asc.sort(ascending=False)
    df_high = df_price_asc.sort(ascending=False)
    df_low = df_price_asc.sort(ascending=False)
    df_open = df_price_asc.sort(ascending=False)
    
    
    db_filename = file_path('db')
    con = sqlite3.connect(db_filename)
    for sym in ls_symbols:
        # Close Data
        close_SQL = """SELECT strftime('%Y-%m-%d %H:%M:%S',Date||'16:00:00') as "Date", Close FROM """+sym+"""_TBL
            WHERE Date BETWEEN '"""+str(dt_start)+"""' AND '"""+str(dt_end)+"""' ORDER BY 1 DESC;
            """

        #df_aa1 = sql.read_frame(close_SQL,con, index_col = 'Date')
        df_aa1 = sql.read_sql_query(close_SQL,con, index_col = 'Date')
        df_aa1.index = pd.to_datetime(df_aa1.index)
        df_aa1['Close'] = df_aa1['Close'].fillna(method='ffill')
        df_aa1['Close'] = df_aa1['Close'].fillna(method='bfill')
        df_aa1['Close'] = df_aa1['Close'].fillna(1.0)
        if not df_aa1.empty:
            for timestamp in ldt_timestamps:
                try:
                    df_close[sym].ix[timestamp] = df_aa1['Close'].ix[timestamp]
                    #print sym,':',timestamp
                except:
                    #print 'Unable to process:', sym,':',timestamp
                    pass
                
        # High Data
        high_SQL = """SELECT strftime('%Y-%m-%d %H:%M:%S',Date||'16:00:00') as "Date", High FROM """+sym+"""_TBL
            WHERE Date BETWEEN '"""+str(dt_start)+"""' AND '"""+str(dt_end)+"""' ORDER BY 1 DESC;
            """

        #df_aa2 = sql.read_frame(high_SQL,con, index_col = 'Date')
        df_aa2 = sql.read_sql_query(high_SQL,con, index_col = 'Date')
        df_aa2.index = pd.to_datetime(df_aa2.index)
        df_aa2['High'] = df_aa2['High'].fillna(method='ffill')
        df_aa2['High'] = df_aa2['High'].fillna(method='bfill')
        df_aa2['High'] = df_aa2['High'].fillna(1.0)
        if not df_aa2.empty:
            for timestamp in ldt_timestamps:
                try:
                    df_high[sym].ix[timestamp] = df_aa2['High'].ix[timestamp]
                    #print sym,':',timestamp
                except:
                    #print 'Unable to process:', sym,':',timestamp
                    pass

        # Low Data
        low_SQL = """SELECT strftime('%Y-%m-%d %H:%M:%S',Date||'16:00:00') as "Date", Low FROM """+sym+"""_TBL
            WHERE Date BETWEEN '"""+str(dt_start)+"""' AND '"""+str(dt_end)+"""' ORDER BY 1 DESC;
            """

        #df_aa3 = sql.read_frame(low_SQL,con, index_col = 'Date')
        df_aa3 = sql.read_sql_query(low_SQL,con, index_col = 'Date')
        df_aa3.index = pd.to_datetime(df_aa3.index)
        df_aa3['Low'] = df_aa3['Low'].fillna(method='ffill')
        df_aa3['Low'] = df_aa3['Low'].fillna(method='bfill')
        df_aa3['Low'] = df_aa3['Low'].fillna(1.0)
        if not df_aa3.empty:
            for timestamp in ldt_timestamps:
                try:
                    df_low[sym].ix[timestamp] = df_aa3['Low'].ix[timestamp]
                    #print sym,':',timestamp
                except:
                    #print 'Unable to process:', sym,':',timestamp
                    pass

        # Open Data
        open_SQL = """SELECT strftime('%Y-%m-%d %H:%M:%S',Date||'16:00:00') as "Date", Open FROM """+sym+"""_TBL
            WHERE Date BETWEEN '"""+str(dt_start)+"""' AND '"""+str(dt_end)+"""' ORDER BY 1 DESC;
            """

        #df_aa4 = sql.read_frame(open_SQL,con, index_col = 'Date')
        df_aa4 = sql.read_sql_query(open_SQL,con, index_col = 'Date')
        df_aa4.index = pd.to_datetime(df_aa4.index)
        df_aa4['Open'] = df_aa4['Open'].fillna(method='ffill')
        df_aa4['Open'] = df_aa4['Open'].fillna(method='bfill')
        df_aa4['Open'] = df_aa4['Open'].fillna(1.0)
        if not df_aa4.empty:
            for timestamp in ldt_timestamps:
                try:
                    df_open[sym].ix[timestamp] = df_aa4['Open'].ix[timestamp]
                    #print sym,':',timestamp
                except:
                    #print 'Unable to process:', sym,':',timestamp
                    pass

    #print 'Data Frame extracted to CSV'
    #df_price.to_csv('./debug/df_price.csv')
    #df_aa1.to_csv('./debug/df_aa1.csv')
    
    #return df_data, ls_symbols
    #df_price.sort_index(axis=0,ascending=False)
    #df_price.to_csv('./debug/df_price.csv')
    
    return df_open,df_high,df_low,df_close, ls_symbols



def get_sqlitedb_data(startdate, enddate, symbol_file, benchmark, price_type):

    print 'This is the sqlite database function'
    
    sym_filepath = file_path('symbol')
    
    ls_symbols = [line.strip() for line in open(sym_filepath+symbol_file+".txt",'r')]
    if benchmark not in ls_symbols:
        ls_symbols.append(benchmark)
    columns = ls_symbols
   

    start_month = startdate.split('/')[0]
    start_day = startdate.split('/')[1]
    start_year = startdate.split('/')[2]

    
    
    end_month = enddate.split('/')[0]
    end_day = enddate.split('/')[1]
    end_year = enddate.split('/')[2]


    dt_start = dt.datetime(int(start_year), int(start_month), int(start_day))
    dt_end = dt.datetime(int(end_year), int(end_month), int(end_day))

    dt_timeofday = dt.timedelta(hours=16)
    

    # Get a list of trading days between the start and the end.
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
    
    #print ldt_timestamps

    df_price_asc = pd.DataFrame(index=ldt_timestamps,columns = columns)
    df_price = df_price_asc.sort(ascending=False)
    
    
    db_filename = file_path('db')
    con = sqlite3.connect(db_filename)
    for sym in ls_symbols:
        read_SQL = """SELECT strftime('%Y-%m-%d %H:%M:%S',Date||'16:00:00') as "Date","""+price_type+""" FROM """+sym+"""_TBL
            WHERE Date BETWEEN '"""+str(dt_start)+"""' AND '"""+str(dt_end)+"""' ORDER BY 1 DESC;
            """

        df_aa1 = sql.read_frame(read_SQL,con, index_col = 'Date')
        df_aa1.index = pd.to_datetime(df_aa1.index)
        df_aa1[price_type] = df_aa1[price_type].fillna(method='ffill')
        df_aa1[price_type] = df_aa1[price_type].fillna(method='bfill')
        df_aa1[price_type] = df_aa1[price_type].fillna(1.0)
        if not df_aa1.empty:
            for timestamp in ldt_timestamps:
                try:
                    df_price[sym].ix[timestamp] = df_aa1['Close'].ix[timestamp]
                    #print sym,':',timestamp
                except:
                    #print 'Unable to process:', sym,':',timestamp
                    pass
                
    #print 'Data Frame extracted to CSV'
    #df_price.to_csv('./debug/df_price.csv')
    #df_aa1.to_csv('./debug/df_aa1.csv')
    
    #return df_data, ls_symbols
    #df_price.sort_index(axis=0,ascending=False)
    #df_price.to_csv('./debug/df_price.csv')
    
    return df_price, ls_symbols

    
def get_data(startdate,enddate,symbol_file, benchmark):
    start_month = startdate.split('/')[0]
    start_day = startdate.split('/')[1]
    start_year = startdate.split('/')[2]
    
    
    end_month = enddate.split('/')[0]
    end_day = enddate.split('/')[1]
    end_year = enddate.split('/')[2]

    # Creating an object of the dataaccess class with Yahoo as the source.
    c_dataobj = da.DataAccess('Yahoo',verbose=True,cachestalltime = 0)
    #c_dataobj = da.DataAccess('Yahoo')
    
    
    # Get the list of symbols
    ls_symbols = c_dataobj.get_symbols_from_list(symbol_file)
    #ls_symbols.append('SPY')
    if benchmark not in ls_symbols:
        ls_symbols.append(benchmark)
    
    # Start and End date of the charts
    dt_start = dt.datetime(int(start_year), int(start_month), int(start_day))
    dt_end = dt.datetime(int(end_year), int(end_month), int(end_day))

    # We need closing prices so the timestamp should be hours=16.
    dt_timeofday = dt.timedelta(hours=16)

    # Get a list of trading days between the start and the end.
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)

    
    # Keys to be read from the data, it is good to read everything in one go.
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']

    # Reading the data, now d_data is a dictionary with the keys above.
    # Timestamps and symbols are the ones that were specified before.
    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))
    
    # Filling the data for NAN
    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)
    
    return d_data, ls_symbols
