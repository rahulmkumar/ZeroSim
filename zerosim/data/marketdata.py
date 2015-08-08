import pandas.io.data as web
import pandas.io.pytables as tab
import datetime

class MarketData(object):

    def get_yahoo_data_type(self, l_sym, start_date, end_date, data_type):
        start_month = int(start_date.split('/')[0])
        start_day = int(start_date.split('/')[1])
        start_year = int(start_date.split('/')[2])

        end_month = int(end_date.split('/')[0])
        end_day = int(end_date.split('/')[1])
        end_year = int(end_date.split('/')[2])

        start = datetime.datetime(start_year, start_month, start_day)
        end = datetime.datetime(end_year, end_month, end_day)

        df_prices = web.DataReader(l_sym, 'yahoo', start, end)

        return df_prices[data_type]

    def get_yahoo_data(self, l_sym, start_date, end_date):
        start_month = int(start_date.split('/')[0])
        start_day = int(start_date.split('/')[1])
        start_year = int(start_date.split('/')[2])

        end_month = int(end_date.split('/')[0])
        end_day = int(end_date.split('/')[1])
        end_year = int(end_date.split('/')[2])

        start = datetime.datetime(start_year, start_month, start_day)
        end = datetime.datetime(end_year, end_month, end_day)

        try:
            df_prices = web.DataReader(l_sym, 'yahoo', start, end)
            return df_prices
        except:
            pass

    def store_hdf5(self, l_sym, start_date, end_date):
        df_prices = self.get_yahoo_data(l_sym, start_date, end_date)

        store = tab.HDFStore('datastore.h5')
        store['prices'] = df_prices
        store.close()

    def get_hdf5(self):

        store = tab.HDFStore('datastore.h5')
        return store['prices']

if __name__ == '__main__':

    data = MarketData()

    #test_data = data.get_yahoo_data(['AAPL','GOOGL'], '01/01/2015', '05/31/2015')

    #data.store_hdf5(['AAPL','GOOGL'],'01/01/2015','01/13/2015')

    data = data.get_hdf5()

    print data['Close']

    #print data.select('prices', "columns=['AAPL', 'GOOGL']")

    #print test_data['AAPL'].ix['Close', :]
    #test_data.to_csv('../../symbols/AAPL_high.csv')