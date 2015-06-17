import urllib2
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd

class Scrape(object):
    SYMBOL_FILES_PATH = '../../symbols/'
    QUANDL_INDICES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/'
    QUANDL_FUTURES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/'
    QUANDL_COMMODITIES = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/'
    QUANDL_CBOE = 'http://www.cboe.com/publish/ScheduledTask/MktData/datahouse/'


    def scrape_page(self, url):
        """
        :param url: URL to scrape using beautiful soup
        :return: a list of JSON objects as a list
        """
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        return soup

    def scrape_remote_file(self, file_url, local_file_path, local_file_name):
        remote_file = urllib2.urlopen(file_url)
        output = open(local_file_path+local_file_name, 'wb')
        output.write(remote_file.read())
        output.close()


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

        pages = range(url_start, url_end, sym_per_page)

        soup = self.scrape_page(header_url)

        #header = soup.find_all("tr",{"align" :"center"})

        # This gets the header items
        # information columns will store: Ticker, Company, Sector, Industry and Country
        info_columns = []

        # Data columns will store: Ticker, Market Cap, P/E, Price, Change and Volume
        data_columns = []

        #find total number of stocks
        total_stocks = int(str(soup.find_all("td", {"class" : "count-text"})[0].contents[1]).split(' ')[0])

        index = range(0, total_stocks)

        #info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[0].text)
        info_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[1].text)
        info_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[2].text)
        info_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[3].text)
        info_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[4].text)
        info_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[5].text)

        #data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text)
        data_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[6].text)
        #data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[7].text)
        data_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[8].text)
        data_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[9].text)
        data_columns.append(soup.find_all("tr", {"align" : "center"})[0].find_all("td", {"style" : "cursor:pointer;"})[10].text)

        #print data_columns

        # first row returns the No. This can become a temporary index in a dataframe
        #Ignore the No.
        #print soup.find_all("td",{"align":"right","class":"body-table-nw"})[0].contents[0]

        # create dataframes
        df_info = pd.DataFrame(index = index, columns = info_columns)
        df_data = pd.DataFrame(index = index, columns = data_columns)

        sym_info_count = range(0, 100, 5)
        sym_data_count = range(0, 115, 6)

        for page in pages[0:3]:
            fetch_url = data_url + str(page)
            print fetch_url

            soup = self.scrape_page(fetch_url)

            snum = 0

            for i in sym_info_count:
                try:
                    info_index = int(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[snum].contents[0])-1
                    df_info[info_columns[0]].ix[info_index] = soup.find_all("td", {"align":"left", "class":"body-table-nw"})[i].contents[0].contents[0]
                    df_info[info_columns[1]].ix[info_index] = soup.find_all("td", {"align":"left", "class":"body-table-nw"})[i+1].contents[0]
                    df_info[info_columns[2]].ix[info_index] = soup.find_all("td", {"align":"left", "class":"body-table-nw"})[i+2].contents[0]
                    df_info[info_columns[3]].ix[info_index] = soup.find_all("td", {"align":"left", "class":"body-table-nw"})[i+3].contents[0]
                    df_info[info_columns[4]].ix[info_index] = soup.find_all("td", {"align":"left", "class":"body-table-nw"})[i+4].contents[0]
                except:
                    print 'Issue with Info count for loop'
                    pass
                snum +=6


            for j in sym_data_count:
                try:
                    data_index = int(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j].contents[0])-1
                    if str(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+1].contents[0]).endswith("B"):
                        df_data[data_columns[0]].ix[data_index] = float(str(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+1].contents[0]).replace('B', ''))*1000
                    elif soup.find_all("td",{"align":"right", "class":"body-table-nw"})[j+1].contents[0] == '-':
                        df_data[data_columns[0]].ix[data_index] = 0
                    else:
                        df_data[data_columns[0]].ix[data_index] = str(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+1].contents[0]).replace('M', '')
                    df_data[data_columns[1]].ix[data_index] = soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+3].contents[0].contents[0]
                    df_data[data_columns[2]].ix[data_index] = float(str(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+4].contents[0].contents[0]).replace('%', ''))
                    df_data[data_columns[3]].ix[data_index] = long(str(soup.find_all("td", {"align":"right", "class":"body-table-nw"})[j+5].contents[0]).replace(',', ''))
                except:
                    pass

            # wait for a random amount of time between 5 and 60 seconds. Overall agerage wait will be 30 seconds per page.
            wait_seconds = random.randint(5, 60)
            time.sleep(wait_seconds)
            print 'waiting for:' + str(wait_seconds)

        df_info.to_csv(file_path +'df_info.csv')
        df_data.to_csv(file_path +'df_data.csv')

    def scrape_quandl_cboe_data(self, file_path=SYMBOL_FILES_PATH):
        pcratio = self.QUANDL_CBOE + 'totalpc.csv'
        paratio_file = 'PCRATIO.csv'

        skew = self.QUANDL_CBOE + 'Skewdailyprices.csv'
        skew_file = 'SKEW.csv'

        file_names = {}

        file_names[1] = [paratio_file, pcratio]
        file_names[2] = [skew_file, skew]

        for key in file_names:
            self.scrape_remote_file(file_names[key][1], file_path, file_names[key][0])


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
            self.scrape_remote_file(file_names[key][1], file_path, file_names[key][0])
