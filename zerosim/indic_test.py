__author__ = 'rahul'
import data
import ta


def main():

    # Return watchlist
    watch_list = data.WatchlistDb()
    watch_list.get_watchlists('data/watchlistdb.db')
    wlist = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')

    # Get data for watchlist
    #wlist = ['AAPL','GOOGL']
    dat = data.MarketData()
    #test_data = dat.get_yahoo_data(ibd50, '01/01/2015', '05/31/2015', 'Close')
    test_data = dat.get_yahoo_data(wlist, '01/01/2015', '05/31/2015')

    ind = ta.Indicators()

    df_bb_u, df_bb_m, df_bb_l = ind.bb(wlist, test_data, 20, 2, 2)
    df_ema = ind.ema(wlist, test_data, 20)
    df_ma = ind.ma(wlist, test_data, 20)
    df_sma = ind.sma(wlist, test_data, 20)

    event = ta.Events()

    df_ma_cross = event.crossabove_df(df_ema, df_sma)

    df_ma_cross.to_csv('df_ma_cross.csv')

    #df_bb_u.to_csv('df_bb_upper.csv')
    #df_bb_m.to_csv('df_bb_ma.csv')
    #df_bb_l.to_csv('df_bb_lower.csv')
    #df_ema.to_csv('df_ema.csv')
    #df_ma.to_csv('df_ma.csv')
    #df_sma.to_csv('df_sma.csv')


if __name__ == '__main__':
    main()
