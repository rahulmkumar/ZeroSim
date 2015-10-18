"""
This is the symbols.py module.

This module is responsible for:
1) Scraping symbols from various web resources: quandl, finviz
2) Creating/maintaining a database for all the symbols scraped
3) Defining user access functions to return a single or a list of symbols.

"""

import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import datetime
from scrape import Scrape


class SymbolDb(object):
    """
    This SymbolDB\b class allows us to:
    1) Scrape the web for symbols
    2) Create.maintain a database of symbols
    3) A user interface for returning a single or list of symbols matching a criteria
    """
    SYMBOL_FILES_PATH = '../symbols/'
    #QUANDL_INDICES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/'
    #QUANDL_FUTURES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/'
    #QUANDL_COMMODITIES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/'

    SYMBOLS_DB = 'symbols.db'
    SYMBOLS_DB_PATH = 'data/'

    def __init__(self):
        """
        This constructor initializes a list of symbols
        """
        self.l_symlist = []

        #http://help.quandl.com/category/183-using-quandl-from-python
        #https://www.quandl.com/tools/python
        self.future_specs = {
            # Corn
            'C': {'MTH': ['H', 'K', 'N', 'U', 'Z'],
                  'EXCH': ['CME'],
                  'EXCHCD': ['CME/C'],
                  'CONTCD': ['CHRIS/CME_C1'],
                  'EXCHFY': ['1959']},

            #Wheat
            'W': {'MTH': ['H', 'K', 'N', 'U', 'Z'],
                  'EXCH': ['CME'],
                  'EXCHCD': ['CME/W'],
                  'CONTCD': ['CHRIS/CME_W1'],
                  'EXCHFY': ['1959']},

            #Soybean
            'S': {'MTH': ['F', 'H', 'K', 'N', 'Q', 'X'],
                  'EXCH': ['CME'],
                  'EXCHCD': ['CME/S'],
                  'CONTCD': ['CHRIS/CME_S1'],
                  'EXCHFY': ['1959']},

            #Soymeal
            'SM': {'MTH': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/SM'],
                   'CONTCD': ['CHRIS/CME_SM1'],
                   'EXCHFY': ['1959']},

            #Soyoil
            'BO': {'MTH': ['F', 'H', 'K', 'N', 'Q', 'X'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/BO'],
                   'CONTCD': ['CHRIS/CME_BO1'],
                   'EXCHFY': ['1959']},

            #Crude Oil
            # Usage CME/CLJ1991
            # Quandl.get("CME/CLJ1991")
            'CL': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/CL'],
                   'CONTCD': ['CHRIS/CME_CL1'],
                   'EXCHFY': ['1983']},

            #RBOB Gasoline
            'RB': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/RB'],
                   'CONTCD': ['CHRIS/CME_RB1'],
                   'EXCHFY': ['2006']},

            #Heating Oil
            'HO': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/HO'],
                   'CONTCD': ['CHRIS/CME_HO1'],
                   'EXCHFY': ['1980']},

            #Natural Gas
            'NG': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/NG'],
                   'CONTCD': ['CHRIS/CME_NG1'],
                   'EXCHFY': ['1990']},

            #Cocoa
            'CC': {'MTH': ['H', 'K', 'N', 'U', 'Z'],
                   'EXCH': ['ICE'],
                   'EXCHCD': ['ICE/CC'],
                   'CONTCD': ['CHRIS/ICE_CC1'],
                   'EXCHFY': ['1990']},

            #Coffee
            'KC': {'MTH': ['H', 'K', 'N', 'U', 'Z'],
                   'EXCH': ['ICE'],
                   'EXCHCD': ['ICE/KC'],
                   'CONTCD': ['CHRIS/ICE_KC1'],
                   'EXCHFY': ['1973']},

            #Cotton
            'CT': {'MTH': ['H', 'K', 'N', 'U', 'Z'],
                   'EXCH': ['ICE'],
                   'EXCHCD': ['ICE/CT'],
                   'CONTCD': ['CHRIS/ICE_CT1'],
                   'EXCHFY': ['1960']},

            #Sugar
            'SB': {'MTH': ['H', 'K', 'N', 'V'],
                   'EXCH': ['ICE'],
                   'EXCHCD': ['ICE/SB'],
                   'CONTCD': ['CHRIS/ICE_SB1'],
                   'EXCHFY': ['1961']},

            #S&P500
            'SP': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/SP'],
                   'CONTCD': ['CHRIS/CME_SP1'],
                   'EXCHFY': ['1982']},

            #Gold
            'GC': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/GC'],
                   'CONTCD': ['CHRIS/CME_GC1'],
                   'EXCHFY': ['1975']},

            #Silver
            'SI': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/SI'],
                   'CONTCD': ['CHRIS/CME_SI1'],
                   'EXCHFY': ['1964']},

            #Copper
            'HG': {'MTH': ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/HG'],
                   'CONTCD': ['CHRIS/CME_HG1'],
                   'EXCHFY': ['1959']},

            #Australian Dollar
            'AD': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/AD'],
                   'CONTCD': ['CHRIS/CME_AD1'],
                   'EXCHFY': ['1987']},

            #British Pound
            'BP': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/BP'],
                   'CONTCD': ['CHRIS/CME_BP1'],
                   'EXCHFY': ['1975']},

            #Euro
            'EC': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/EC'],
                   'CONTCD': ['CHRIS/CME_EC1'],
                   'EXCHFY': ['1999']},

            #Japanese Yen
            'JY': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['CME'],
                   'EXCHCD': ['CME/JY'],
                   'CONTCD': ['CHRIS/CME_JY1'],
                   'EXCHFY': ['1977']},

            #Dollar Index
            'DX': {'MTH': ['H', 'M', 'U', 'Z'],
                   'EXCH': ['ICE'],
                   'EXCHCD': ['ICE/DX'],
                   'CONTCD': ['CHRIS/ICE_DX1'],
                   'EXCHFY': ['1986']}
        }

    def generate_futures_symbols_exchange(self, sym, start_date, end_date):

        start_year = int(start_date.split('/')[2])
        end_year = int(end_date.split('/')[2])

        years = range(int(start_year), int(end_year), 1)

        for sym in sym:
            contract_months = self.future_specs[sym]['MTH']
            sym_prefix = self.future_specs[sym]['EXCHCD'][0]
            earliest_year = self.future_specs[sym]['EXCHFY'][0]

            if int(start_year) < int(earliest_year):
                print 'Incorrect start year for symbol:' + sym
                pass

        return [sym_prefix+mth+str(year) for mth in contract_months for year in years]

    def generate_futures_symbols_continuous(self, sym):

        return [self.future_specs[sym]['CONTCD'][0] for sym in sym]


    def merge_symbol_files_to_db(self, file_path=SYMBOL_FILES_PATH, db_path=SYMBOLS_DB_PATH, db_name=SYMBOLS_DB):
        """
        This function merges all downloaded files into a SQlite database
        :param file_path: Path of the scraped files
        :param db_path: Directory to create the database in
        :param db_name: Name of the SqLite database
        :return:
        """
        df_data = pd.read_csv(file_path + 'df_data.csv', index_col=0)
        df_info = pd.read_csv(file_path + 'df_info.csv', index_col=0)
        df_sp500 = pd.read_csv(file_path + 'SP500.csv', index_col=0)
        df_djia = pd.read_csv(file_path + 'DJIA.csv', index_col=0)
        df_nyse = pd.read_csv(file_path + 'NYSE.csv', index_col=0)
        df_nasdaq = pd.read_csv(file_path + 'NASDAQ.csv', index_col=0)
        df_nyse100 = pd.read_csv(file_path + 'NYSE100.csv', index_col=0)
        df_nasdaq100 = pd.read_csv(file_path + 'NASD100.csv', index_col=0)

        # SQlite database connection
        engine = create_engine('sqlite:///'+db_path+db_name)

        #Create finviz table in database
        df_merged = pd.merge(df_info, df_data, left_index=True, right_index=True)
        df_finviz = df_merged.set_index('Ticker')
        df_finviz.to_sql('finviz', engine, if_exists='replace')

        #Create nyse table in database
        df_nyse['Exchange'] = 'NYSE'
        df_nyse.to_sql('nyse', engine, if_exists='replace')

        #Create nasdaq table in database
        df_nasdaq['Exchange'] = 'NASDAQ'
        df_nasdaq.to_sql('nasdaq', engine, if_exists='replace')

        #Create S&P500 table in database
        df_sp500['Index'] = 'SP500'
        df_sp500.to_sql('sp500', engine, if_exists='replace')

        #Create nyse100 table in database
        df_nyse100['Index'] = 'NYSE100'
        df_nyse100.to_sql('nyse100', engine, if_exists='replace')

        #Create nasdaq100 table in database
        df_nasdaq100['Index'] = 'NASD100'
        df_nasdaq100.to_sql('nasdaq100', engine, if_exists='replace')

        con = sqlite3.connect('symbols.db')
        drop_table_query = """ DROP TABLE US_STOCK_TBL;"""
        create_table_query = """ CREATE TABLE US_STOCK_TBL(
                                "Ticker" TEXT,
                                "Code" TEXT,
                                "Exchange" TEXT,
                                "Index" TEXT,
                                "Company" TEXT,
                                "Sector" TEXT,
                                "Industry" TEXT,
                                "Country" TEXT,
                                "MarketCap" FLOAT,
                                "Change" FLOAT,
                                "Price" FLOAT,
                                "Volume" FLOAT);"""

        populate_sql_query = """
            INSERT INTO US_STOCK_TBL
            (Ticker,Code,Exchange,[Index],Company,Sector,Industry,Country,MarketCap,Change,Price,Volume)
            SELECT finviz.Ticker,
            CASE
            WHEN nyse.Code NOT NULL THEN nyse.Code
            WHEN nasdaq.Code NOT NULL THEN nasdaq.Code
            END as "Code",
            CASE
            WHEN nyse.Code NOT NULL THEN 'NYSE'
            WHEN nasdaq.Code NOT NULL THEN 'NASDAQ'
            END as "Exchange",
            CASE
            WHEN sp500.Code NOT NULL THEN 'SP500'
            WHEN nyse100.Code NOT NULL THEN 'NYSE100'
            WHEN nasdaq100.Code NOT NULL THEN 'NASDAQ100'
            END
            as "Index", finviz.Company, finviz.Sector, finviz.Industry, finviz.Country, finviz.[Market Cap] as "MarketCap", finviz.Change, finviz.Price, finviz.Volume
            FROM finviz
            LEFT OUTER JOIN nyse ON finviz.Ticker = nyse.Ticker
            LEFT OUTER JOIN nasdaq ON finviz.Ticker = nasdaq.Ticker
            LEFT OUTER JOIN sp500 ON finviz.Ticker = sp500.Ticker
            LEFT OUTER JOIN nyse100 ON finviz.Ticker = nyse100.Ticker
            LEFT OUTER JOIN nasdaq100 ON finviz.Ticker = nasdaq100.Ticker;
            """
        try:
            con.execute(drop_table_query)
            con.commit()
        except:
            pass
        con.execute(create_table_query)
        con.commit()
        con.execute(populate_sql_query)
        con.commit()
        con.close()

    def get_symbols(self, source, file_path = SYMBOL_FILES_PATH, db_path=SYMBOLS_DB_PATH, db_name=SYMBOLS_DB, **kwargs):
        engine = create_engine('sqlite:///'+db_path+db_name)
        read_sql_query = """ SELECT Ticker,Code,Exchange,[Index],Company,Sector,Industry,Country,MarketCap,Change,Price,Volume FROM US_STOCK_TBL"""
        df_final = pd.read_sql(read_sql_query, engine)
        df_final = df_final.set_index('Ticker')
        df_final.to_csv(file_path + 'df_final.csv')

        sql_query = ''
        if source == 'Quandl':
            sql_query = """SELECT Code FROM US_STOCK_TBL"""
        elif source == 'Yahoo':
            sql_query = """SELECT Ticker FROM US_STOCK_TBL"""

        try:
            sql_query = sql_query + """ WHERE Sector LIKE '%"""+kwargs['Sector']+"""%'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Industry LIKE '%"""+kwargs['Industry']+"""%'"""
            else:
                sql_query = sql_query + """ WHERE Industry LIKE '%"""+kwargs['Industry']+"""%'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Country = '"""+kwargs['Country']+"""'"""
            else:
                sql_query = sql_query + """ WHERE Country = '"""+kwargs['Country']+"""'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Exchange = '"""+kwargs['Exchange']+"""'"""
            else:
                sql_query = sql_query + """ WHERE Exchange = '"""+kwargs['Exchange']+"""'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Index = '"""+kwargs['Index']+"""'"""
            else:
                sql_query = sql_query + """ WHERE Index = '"""+kwargs['Index']+"""'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                #sql_query = sql_query + """ AND Mcap > """+kwargs['Mcap']
                sql_query = sql_query + """ AND MarketCap BETWEEN '"""+str(kwargs['Mcap'][0])+"""' AND '"""+str(kwargs['Mcap'][1])+"""'"""
            else:
                #sql_query = sql_query + """ WHERE Mcap > """+kwargs['Mcap']
                sql_query = sql_query + """ WHERE MarketCap BETWEEN '"""+str(kwargs['Mcap'][0])+"""' AND '""" + str(kwargs['Mcap'][1])+"""'"""
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Change > """+kwargs['Change']
            else:
                sql_query = sql_query + """ WHERE Change > """+kwargs['Change']
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Volume > """+kwargs['Volume']
            else:
                sql_query = sql_query + """ WHERE Volume > """+kwargs['Volume']
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Price < """+kwargs['Pricelt']
            else:
                sql_query = sql_query + """ WHERE Price < """+kwargs['Pricelt']
        except:
            pass

        try:
            if sql_query.find('WHERE') <> -1:
                sql_query = sql_query + """ AND Price > """+kwargs['Pricegt']
            else:
                sql_query = sql_query + """ WHERE Price > """+kwargs['Pricegt']
        except:
            pass

        if source == 'Quandl':
            sql_query = sql_query + """ AND Code NOT NULL;"""
        elif source == 'Yahoo':
            sql_query = sql_query + """ AND Ticker NOT NULL;"""

        df_final = pd.read_sql(sql_query, engine)

        df_final.to_csv(file_path + 'SQL_generated.csv')

        if source =='Quandl':
            return [str(item) for item in list(df_final['Code'])]
        elif source == 'Yahoo':
            return [str(item) for item in list(df_final['Ticker'])]



if __name__ == '__main__':

    current_time = datetime.datetime.now().time()
    print 'Start time:' + str(current_time)

    scrape = Scrape()
    sym = SymbolDb()

    # Refresh symbol files from Quandl link
    #scrape.scrape_quandl_codes_us()
    #scrape.scrape_quandl_cboe_data()

    #change total pages to scrape in function above
    #scrape.scrape_finviz_codes_overview(7141,20)

    #scrape.scrape_finviz_codes_overview()

    # Merge all the symbol files from finviz and quandl into SQLite database
    #sym.merge_symbol_files_to_db()

    # Returns the final table from the database
    #sym.get_symbols()

    #scrape Indian stock symbols
    #file_url_nse = 'https://www.quandl.com/api/v2/datasets.csv?query=*&source_code=NSE&per_page=300&page='
    #file_url_bse = 'https://www.quandl.com/api/v2/datasets.csv?query=*&source_code=BSE&per_page=300&page='
    #scrape.scrape_remote_file_by_page(file_url_nse, sym.SYMBOL_FILES_PATH, 'NSE.csv')
    #scrape.scrape_remote_file_by_page(file_url_bse, sym.SYMBOL_FILES_PATH, 'BSE.csv')

    #scrape.scrape_remote_file(file_url+str(1), sym.SYMBOL_FILES_PATH, 'NSE.csv')

    #sym.get_symbols(source='Quandl', Country='USA', Volume='1000000', Industry='Biotech')

    print sym.generate_futures_symbols_exchange('C', '01/01/1960', '12/31/2015')
    print sym.generate_futures_symbols_continuous(['C', 'S'])
    end_time = datetime.datetime.now().time()
    print 'End time:'+str(end_time)


