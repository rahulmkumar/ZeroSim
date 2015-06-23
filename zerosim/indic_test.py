__author__ = 'rahul'
import data
import ta


def main():

    # Return watchlist
    watch_list = data.WatchlistDb()
    watch_list.get_watchlists('data/watchlistdb.db')
    ibd50 = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')

    # Get data for watchlist
    dat = data.MarketData()
    test_data = dat.get_yahoo_data(ibd50, '01/01/2015', '05/31/2015', 'Close')
    #test_data = dat.get_yahoo_data(['AAPL'], '01/01/2015', '05/31/2015', 'Close')

    ind = ta.Indicators()

    df_bb_u, df_bb_m, df_bb_l = ind.talib_BB(test_data, 20, 2, 2)

    df_bb_u.to_csv('df_bb_upper.csv')
    df_bb_m.to_csv('df_bb_ma.csv')
    df_bb_l.to_csv('df_bb_lower.csv')


if __name__ == '__main__':
    main()
