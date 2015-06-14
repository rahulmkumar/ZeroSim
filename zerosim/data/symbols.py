import urllib2
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from sqlalchemy import create_engine
import sqlite3

class SymbolDb(object):

    SYMBOL_FILES_PATH = '../../symbols/'

    def __init__(self):
        self.l_symlist = []

    def refresh_quandl_symbol_files(self,path):
        self.get_quandl_codes_us(path)

    def scrape_page(self,url):
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        return soup

    def scrape_finviz_codes_overview(self,url_end,sym_per_page):

        header_url = "http://www.finviz.com/screener.ashx?v=111&r=1"
        data_url = "http://www.finviz.com/screener.ashx?v=111&r="

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

        df_info.to_csv(self.SYMBOL_FILES_PATH +'df_info.csv')
        df_data.to_csv(self.SYMBOL_FILES_PATH +'df_data.csv')



# The code files for this function were taken from: https://www.quandl.com/resources/useful-lists
    def get_quandl_codes_us(self):
        sp500 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/SP500.csv'
        sp500_file = 'SP500.csv'

        djia = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/dowjonesIA.csv'
        djia_file = 'DJIA.csv'

        nasd = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/NASDAQComposite.csv'
        nasd_file = 'NASDAQ.csv'

        nasd100 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/nasdaq100.csv'
        nasd100_file = 'NASD100.csv'

        nyse = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/NYSEComposite.csv'
        nyse_file = 'NYSE.csv'

        nyse100 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/nyse100.csv'
        nyse100_file = 'NYSE100.csv'

        futures = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/meta.csv'
        futures_file = 'FUTURES.csv'

        commodities = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/commodities.csv'
        commodities_file = 'COMMODITIES.csv'

        file_names = {}
        file_names[1] = [sp500_file,sp500]
        file_names[2] = [djia_file,djia]
        file_names[3] = [nasd_file,nasd]
        file_names[4] = [nasd100_file,nasd100]
        file_names[5] = [nyse_file,nyse]
        file_names[6] = [nyse100_file,nyse100]
        file_names[7] = [futures_file,futures]
        file_names[8] = [commodities_file,commodities]


        for key in file_names:
            symfile = urllib2.urlopen(file_names[key][1])
            output = open(self.SYMBOL_FILES_PATH+file_names[key][0],'wb')
            output.write(symfile.read())
            output.close()


    def merge_symbol_files(self):
        df_data = pd.read_csv(self.SYMBOL_FILES_PATH + 'df_data.csv', index_col=0)
        df_info = pd.read_csv(self.SYMBOL_FILES_PATH + 'df_info.csv', index_col=0)
        df_sp500 = pd.read_csv(self.SYMBOL_FILES_PATH + 'SP500.csv', index_col=0)
        df_djia = pd.read_csv(self.SYMBOL_FILES_PATH + 'DJIA.csv', index_col=0)
        df_nyse = pd.read_csv(self.SYMBOL_FILES_PATH + 'NYSE.csv', index_col=0)
        df_nasdaq = pd.read_csv(self.SYMBOL_FILES_PATH + 'NASDAQ.csv', index_col=0)
        df_nyse100 = pd.read_csv(self.SYMBOL_FILES_PATH + 'NYSE100.csv', index_col=0)
        df_nasdaq100 = pd.read_csv(self.SYMBOL_FILES_PATH + 'NASD100.csv', index_col=0)


        # SQlite database connection
        engine = create_engine('sqlite:///symbols.db')

        #Create finviz table in database
        df_merged = pd.merge(df_info,df_data, left_index=True, right_index=True)
        df_finviz = df_merged.set_index('Ticker')
        df_finviz.to_sql('finviz',engine,if_exists='replace')

        #Create nyse table in database
        df_nyse['Exchange'] = 'NYSE'
        df_nyse.to_sql('nyse',engine,if_exists='replace')

        #Create nyse table in database
        df_nasdaq['Exchange'] = 'NASDAQ'
        df_nasdaq.to_sql('nasdaq',engine,if_exists='replace')

        #Create nyse table in database
        df_sp500['Index'] = 'SP500'
        df_sp500.to_sql('sp500',engine,if_exists='replace')

        #Create nyse table in database
        df_nyse100['Index'] = 'NYSE100'
        df_nyse100.to_sql('nyse100',engine,if_exists='replace')

        #Create nyse table in database
        df_nasdaq100['Index'] = 'NASD100'
        df_nasdaq100.to_sql('nasdaq100',engine,if_exists='replace')

        con = sqlite3.connect('symbols.db')
        drop_table_query = """ DROP TABLE final;"""
        create_table_query = """ CREATE TABLE final(
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
            INSERT INTO final
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

    def get_symbols(self):
        engine = create_engine('sqlite:///symbols.db')
        read_sql_query = """ SELECT Ticker,Code,Exchange,[Index],Company,Sector,Industry,Country,MarketCap,Change,Price,Volume FROM final"""
        df_final = pd.read_sql(read_sql_query,engine)
        df_final = df_final.set_index('Ticker')
        df_final.to_csv(self.SYMBOL_FILES_PATH + 'df_final.csv')
        #engine.close()

if __name__ == '__main__':
    sym = SymbolDb()

    # Refresh symbol files from Quandl link
    sym.get_quandl_codes_us()

    #change total pages to scrape in function above
    sym.scrape_finviz_codes_overview(7141,20)

    # Merge all the symbol files from finviz and quandl into SQLite database
    sym.merge_symbol_files()

    # Returns the final table from the database
    sym.get_symbols()


