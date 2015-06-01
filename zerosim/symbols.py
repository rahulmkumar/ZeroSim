import urllib2

class SymbolDb:
"""
This Class is used to scrape and maintain a database of all symbols
"""
    def __init__(self):
        self.l_symlist = []

    def refreshSymbols(self,path):
        self.get_quandl_codes(path)

    def get_quandl_codes(self,file_path):
        self.sp500 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/SP500.csv'
        self.sp500_file = 'SP500.csv'

        self.djia = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/dowjonesIA.csv'
        self.djia_file = 'DJIA.csv'

        self.nasd = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/NASDAQComposite.csv'
        self.nasd_file = 'NASDAQ.csv'

        self.nasd100 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/nasdaq100.csv'
        self.nasd100_file = 'NASD100.csv'

        self.nyse = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/NYSEComposite.csv'
        self.nyse_file = 'NYSE.csv'

        self.nyse100 = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/nyse100.csv'
        self.nyse100_file = 'NYSE100.csv'

        self.futures = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Futures/meta.csv'
        self.futures_file = 'FUTURES.csv'

        self.commodities = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/commodities.csv'
        self.commodities_file = 'COMMODITIES.csv'

        self.file_names = {}
        self.file_names[1] = [self.sp500_file,self.sp500]
        self.file_names[2] = [self.djia_file,self.djia]
        self.file_names[3] = [self.nasd_file,self.nasd]
        self.file_names[4] = [self.nasd100_file,self.nasd100]
        self.file_names[5] = [self.nyse_file,self.nyse]
        self.file_names[6] = [self.nyse100_file,self.nyse100]
        self.file_names[7] = [self.futures_file,self.futures]
        self.file_names[8] = [self.commodities_file,self.commodities]


        for key in self.file_names:
            symfile = urllib2.urlopen(self.file_names[key][1])
            output = open(file_path+self.file_names[key][0],'wb')
            output.write(symfile.read())
            output.close()


if __name__ == '__main__':
    sym = SymbolDb()
    sym.get_quandl_codes('../symbols/')

