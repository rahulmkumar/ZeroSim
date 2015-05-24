import marketdata as md
import YahooDataPull as yp
import time
import sys


def main():
    
    load_file = sys.argv[1]
    
    md.read_finviz_US(load_file)
    md.read_quandl_US(load_file)
    md.combine_tickers_US(load_file)
    
    start_time_data = time.time()
    md.update_yahoo_files(load_file)
    print load_file, ' Data Update:', time.time() - start_time_data
    
    sym_list = md.create_sqlite_tables(load_file)
    md.load_sqlite_data(sym_list)
    print 'Sqlite Database Updated with ',load_file,' data'
        
if __name__ == '__main__':
    main()    