__author__ = 'rahul'
import data
import ta
import datetime



def main():

    current_time = datetime.datetime.now().time()
    print 'Start time:' + str(current_time)

    # Return watchlist
    watch_list = data.WatchlistDb()
    watch_list.get_watchlists('data/watchlistdb.db')
    wlist = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')
    #wlist = ['AAPL','GOOGL']

    # Get data for watchlist
    dat = data.MarketData()
    #test_data = dat.get_yahoo_data_type(ibd50, '01/01/2015', '05/31/2015', 'Close')
    test_data = dat.get_yahoo_data(wlist, '02/01/2015', '06/25/2015')

    ind = ta.Indicators()

    # Bollinger Bands
    df_bb_u, df_bb_m, df_bb_l = ind.bb(wlist, test_data['Close'], 20, 2, 2)
    #df_bb_u.to_csv('df_bb_upper.csv')
    #df_bb_m.to_csv('df_bb_ma.csv')
    #df_bb_l.to_csv('df_bb_lower.csv')

    # Fibonacci EMA's
    df_ema8 = ind.ema(wlist, test_data['Close'], 8)
    df_ema21 = ind.ema(wlist, test_data['Close'], 21)
    df_ema34 = ind.ema(wlist, test_data['Close'], 34)
    df_ema55 = ind.ema(wlist, test_data['Close'], 55)
    df_ema89 = ind.ema(wlist, test_data['Close'], 89)

    #df_ema8.to_csv('df_ema8.csv')
    #df_ema21.to_csv('df_ema21.csv')
    #df_ema34.to_csv('df_ema34.csv')
    #df_ema55.to_csv('df_ema55.csv')
    #df_ema89.to_csv('df_ema89.csv')

    # Test EMA's
    #df_ema_old = ind.ema_old(close_data, 21)
    #df_ema_old.to_csv('df_ema_old.csv')

    #df_ma = ind.ma(wlist, test_data, 20)
    #df_ma.to_csv('df_ma.csv')

    #df_sma = ind.sma(wlist, test_data, 20)
    #df_sma.to_csv('df_sma.csv')

    # Keltner Channels
    df_kelt_u, df_kelt_m, df_kelt_l = ind.keltner(wlist, test_data['High'], test_data['Low'], test_data['Close'], 20, 20, 2)
    #df_kelt_u.to_csv('df_kelt_u.csv')
    #df_kelt_m.to_csv('df_kelt_m.csv')
    #df_kelt_l.to_csv('df_kelt_l.csv')

    # ATR
    #df_atr = ind.atr(wlist, test_data, 14)
    #df_atr.to_csv('df_atr.csv')

    # TTM Squeeze Test
    df_bb_ma, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l = ind.ttm_squeeze(wlist, test_data['High'], test_data['Low'], test_data['Close'], 21, 2, 21, 21, 1.5)
    #df_bb_ma.to_csv('df_bb_ma.csv')
    #df_bb_u.to_csv('df_bb_u.csv')
    #df_bb_l.to_csv('df_bb_l.csv')
    #df_kch_m.to_csv('df_kch_m.csv')
    #df_kch_u.to_csv('df_kch_u.csv')
    #df_kch_l.to_csv('df_kch_l.csv')

    # Ichimoku Test
    df_ichi_tenkan, df_ichi_kijun = ind.ichimoku(wlist, test_data['High'], test_data['Low'])
    #df_ichi_tenkan.to_csv('df_ichi_tenkan.csv')
    #df_ichi_kijun.to_csv('df_ichi_kijun.csv')

    # Momentum
    df_mom = ind.mom(wlist, test_data['Close'], 12)
    #df_mom.to_csv('df_mom.csv')

    # ADX
    df_adx = ind.adx(wlist, test_data['High'], test_data['Low'], test_data['Close'], 14)
    #df_adx.to_csv('df_adx.csv')

    # Event Scanner
    event = ta.Events()
    cross_sym = event.crossabove_scan(df_ema8, df_ema21)
    print 'EMA8 X EMA21:'+ str(cross_sym)

    # Ichimoku Cross
    cross_ichimoku = event.crossabove_scan(df_ichi_tenkan, df_ichi_kijun)
    print 'Tenkan X Kijun:' + str(cross_ichimoku)

    # Price crosses over Kijun Sen
    cross_kijun = event.crossabove_scan(test_data['Close'], df_ichi_kijun)
    print 'Price X Kijun:' + str(cross_kijun)

    # TTM squeeze firing
    ttm_cross_u = event.crossabove_scan(df_bb_u, df_kch_u)
    print 'TTM Upper Cross: ' + str(ttm_cross_u)

    # MACD Histogram Rising
    macd_rising = event.rising_scan(test_data['Close'])
    print 'MACD Histogram Rising:' + str(macd_rising)

    current_time = datetime.datetime.now().time()
    print 'End time:' + str(current_time)

if __name__ == '__main__':
    main()
