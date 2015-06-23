import pandas.io.data as web
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

        df_prices = web.DataReader(l_sym, 'yahoo', start, end)
        df_prices = df_prices.swapaxes('items', 'minor')

        #print df_prices['AAPL'].ix[:, 'Close']
        #print df_prices['AAPL'].index

        return df_prices


if __name__ == '__main__':

    data = MarketData()

    test_data = data.get_yahoo_data(['AAPL','GOOGL'], '01/01/2015', '05/31/2015')
    #print test_data['AAPL'].ix['Close', :]
    #test_data.to_csv('../../symbols/AAPL_high.csv')

