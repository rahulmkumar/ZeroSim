"""
This is the symbols.py module.

This module is responsible for:
1) Scraping symbols from various web resources: quandl, finviz
2) Creating/maintaining a database for all the symbols scraped
3) Defining user access functions to return a single or a list of symbols.

"""

import urllib2
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from sqlalchemy import create_engine
import sqlite3
import datetime


class SymbolDb(object):
    """
    This SymbolDB\b class allows us to:
    1) Scrape the web for symbols
    2) Create.maintain a database of symbols
    3) A user interface for returning a single or list of symbols matching a criteria
    """
    SYMBOL_FILES_PATH = '../../symbols/'
    QUANDL_INDICES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/'
    QUANDL_FUTURES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/'
    QUANDL_COMMODITIES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/'

    SYMBOLS_DB = 'symbols.db'
    SYMBOLS_DB_PATH = '/'



    def __init__(self):
        """
        This constructor initializes a list of symbols
        """
        self.l_symlist = []

    def scrape_page(self, url):
        """
        :param url: URL to scrape using beautiful soup
        :return: a list of JSON objects as a list
        """
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        return soup

    def scrape_finviz_codes_overview(self, url_end=7141, sym_per_page=20, file_path=SYMBOL_FILES_PATH):
        """
        :param url_end: Total number of stocks + 20
        :param sym_per_page: Total number of symbols per page on the finviz.com screener tab
        :param file_path: Path to store the file of symbols
        :return:
        """

        data_url = "http://www.finviz.com/screener.ashx?v=111&r="

        header_url = data_url + "1"
        url_start = 1
        #url_end = 7141
        #sym_per_page = 20

        pages = range(url_start,url_end,sym_per_page)

        soup = self.scrape_page(header_url)

        #header = soup.find_all("tr",{"align" :"center"})

        # This gets the header items
        # information columns will store: Ticker, Company, Sector, Industry and Country
        info_columns = []

        # Data columns will store: Ticker, Market Cap, P/E, Price, Change and Volume
        data_columns = []

        #find total number of stocks
        total_stocks = int(str(soup.find_all("td",{"class" : "count-text"})[0].contents[1]).split(' ')[0])

        index = range(0,total_stocks)

        #info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[0].text)
        info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text)
        info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[2].text)
        info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[3].text)
        info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[4].text)
        info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[5].text)

        #data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text)
        data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[6].text)
        #data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[7].text)
        data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[8].text)
        data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[9].text)
        data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[10].text)

        #print data_columns

        # first row returns the No. This can become a temporary index in a dataframe
        #Ignore the No.
        #print soup.find_all("td",{"align":"right","class":"body-table-nw"})[0].contents[0]

        # create dataframes
        df_info = pd.DataFrame(index = index, columns = info_columns)
        df_data = pd.DataFrame(index = index, columns = data_columns)

        sym_info_count = range(0,100,5)
        sym_data_count = range(0,115,6)

        for page in pages[0:3]:
            fetch_url = data_url + str(page)
            print fetch_url

            soup = self.scrape_page(fetch_url)

            snum = 0

            for i in sym_info_count:
                try:
                    info_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[snum].contents[0])-1
                    df_info[info_columns[0]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i].contents[0].contents[0]
                    df_info[info_columns[1]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+1].contents[0]
                    df_info[info_columns[2]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+2].contents[0]
                    df_info[info_columns[3]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+3].contents[0]
                    df_info[info_columns[4]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+4].contents[0]
                except:
                    print 'Issue with Info count for loop'
                    pass
                snum +=6


            for j in sym_data_count:
                try:
                    data_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j].contents[0])-1
                    if str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).endswith("B"):
                        df_data[data_columns[0]].ix[data_index] = float(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).replace('B',''))*1000
                    elif soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0] == '-':
                        df_data[data_columns[0]].ix[data_index] = 0
                    else:
                        df_data[data_columns[0]].ix[data_index] = str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).replace('M','')
                    df_data[data_columns[1]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+3].contents[0].contents[0]
                    df_data[data_columns[2]].ix[data_index] = float(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+4].contents[0].contents[0]).replace('%',''))
                    df_data[data_columns[3]].ix[data_index] = long(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+5].contents[0]).replace(',',''))
                except:
                    pass

            # wait for a random amount of time between 5 and 60 seconds. Overall agerage wait will be 30 seconds per page.
            wait_seconds = random.randint(5,60)
            time.sleep(wait_seconds)
            print 'waiting for:' + str(wait_seconds)

        df_info.to_csv(file_path +'df_info.csv')
        df_data.to_csv(file_path +'df_data.csv')




    def scrape_quandl_codes_us(self, file_path=SYMBOL_FILES_PATH):
        """
        The code files for this function were taken from: https://www.quandl.com/resources/useful-lists
        :param file_path:Path to store the scraped files
        :return:
        """

        sp500 = self.QUANDL_INDICES + 'SP500.csv'
        sp500_file = 'SP500.csv'

        djia = self.QUANDL_INDICES + 'dowjonesIA.csv'
        djia_file = 'DJIA.csv'

        nasd = self.QUANDL_INDICES + 'NASDAQComposite.csv'
        nasd_file = 'NASDAQ.csv'

        nasd100 = self.QUANDL_INDICES + 'nasdaq100.csv'
        nasd100_file = 'NASD100.csv'

        nyse = self.QUANDL_INDICES + 'NYSEComposite.csv'
        nyse_file = 'NYSE.csv'

        nyse100 = self.QUANDL_INDICES + 'nyse100.csv'
        nyse100_file = 'NYSE100.csv'

        futures = self.QUANDL_FUTURES + 'meta.csv'
        futures_file = 'FUTURES.csv'

        commodities = self.QUANDL_COMMODITIES + 'commodities.csv'
        commodities_file = 'COMMODITIES.csv'

        file_names = {}
        file_names[1] = [sp500_file, sp500]
        file_names[2] = [djia_file, djia]
        file_names[3] = [nasd_file, nasd]
        file_names[4] = [nasd100_file, nasd100]
        file_names[5] = [nyse_file, nyse]
        file_names[6] = [nyse100_file, nyse100]
        file_names[7] = [futures_file, futures]
        file_names[8] = [commodities_file, commodities]


        for key in file_names:
            symfile = urllib2.urlopen(file_names[key][1])
            output = open(file_path+file_names[key][0], 'wb')
            output.write(symfile.read())
            output.close()


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
        engine = create_engine('sqlite://'+db_path+db_name)

        #Create finviz table in database
        df_merged = pd.merge(df_info, df_data, left_index=True, right_index=True)
        df_finviz = df_merged.set_index('Ticker')
        df_finviz.to_sql('finviz', engine, if_exists='replace')

        #Create nyse table in database
        df_nyse['Exchange'] = 'NYSE'
        df_nyse.to_sql('nyse', engine, if_exists='replace')

        #Create nyse table in database
        df_nasdaq['Exchange'] = 'NASDAQ'
        df_nasdaq.to_sql('nasdaq', engine, if_exists='replace')

        #Create nyse table in database
        df_sp500['Index'] = 'SP500'
        df_sp500.to_sql('sp500', engine, if_exists='replace')

        #Create nyse table in database
        df_nyse100['Index'] = 'NYSE100'
        df_nyse100.to_sql('nyse100', engine, if_exists='replace')

        #Create nyse table in database
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

        #engine.close()

    def get_symbols(self, file_path = SYMBOL_FILES_PATH, db_path=SYMBOLS_DB_PATH, db_name=SYMBOLS_DB):
        engine = create_engine('sqlite://'+db_path+db_name)
        read_sql_query = """ SELECT Ticker,Code,Exchange,[Index],Company,Sector,Industry,Country,MarketCap,Change,Price,Volume FROM US_STOCK_TBL"""
        df_final = pd.read_sql(read_sql_query, engine)
        df_final = df_final.set_index('Ticker')
        df_final.to_csv(file_path + 'df_final.csv')
        #engine.close()

if __name__ == '__main__':

    current_time = datetime.datetime.now().time()
    print 'Start time:' + str(current_time)

    sym = SymbolDb()

    # Refresh symbol files from Quandl link
    sym.scrape_quandl_codes_us()

    #change total pages to scrape in function above
    #sym.scrape_finviz_codes_overview(7141,20)
    sym.scrape_finviz_codes_overview()

    # Merge all the symbol files from finviz and quandl into SQLite database
    sym.merge_symbol_files_to_db()

    # Returns the final table from the database
    sym.get_symbols()

    end_time = datetime.datetime.now().time()
    print 'End time:'+str(end_time)


