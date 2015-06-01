import urllib2

class SymbolDb:
    def __init__(self):
        self.l_symlist = []

    def refreshSymbolFiles(self,path):
        self.get_quandl_codes_us(path)

    def get_quandl_codes_us(self,file_path):
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
            output = open(file_path+file_names[key][0],'wb')
            output.write(symfile.read())
            output.close()


if __name__ == '__main__':
    sym = SymbolDb()
    sym.refreshSymbolFiles('../symbols/')

