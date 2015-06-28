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
    ibd50 = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')
    biotech = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'Biotech')
    ETFOptions = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'ETFOptions')
    wlist = ibd50 + biotech + ETFOptions

    # Get data for watchlist
    dat = data.MarketData()
    test_data = dat.get_yahoo_data(wlist, '10/01/2014', '06/25/2015')

    ind = ta.Indicators()

    # Bollinger Bands
    df_bb_u, df_bb_m, df_bb_l = ind.bb(wlist, test_data['Close'], 20, 2, 2)

    # Fibonacci EMA's
    df_ema8 = ind.ema(wlist, test_data['Close'], 8)
    df_ema21 = ind.ema(wlist, test_data['Close'], 21)
    df_ema34 = ind.ema(wlist, test_data['Close'], 34)
    df_ema55 = ind.ema(wlist, test_data['Close'], 55)
    df_ema89 = ind.ema(wlist, test_data['Close'], 89)

    # Keltner Channels
    df_kelt_u, df_kelt_m, df_kelt_l = ind.keltner(wlist, test_data['High'], test_data['Low'], test_data['Close'], 20, 20, 2)

    # TTM Squeeze Test
    df_bb_ma, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l = ind.ttm_squeeze(wlist, test_data['High'], test_data['Low'], test_data['Close'], 21, 2, 21, 21, 1.5)

    # Ichimoku Test
    df_ichi_tenkan, df_ichi_kijun = ind.ichimoku(wlist, test_data['High'], test_data['Low'])

    # Momentum
    df_mom = ind.mom(wlist, test_data['Close'], 12)

    # ADX
    df_adx = ind.adx(wlist, test_data['High'], test_data['Low'], test_data['Close'], 14)

    # Event Scanner
    event = ta.Events()
    cross_sym = event.crossabove_scan(df_ema8, df_ema21)
    print 'EMA8 X EMA21:'+ str(cross_sym)

    # Ichimoku Cross
    cross_ichimoku = event.crossabove_scan(df_ichi_tenkan, df_ichi_kijun)
    print 'Tenkan X Kijun(ETF):' + str(cross_ichimoku)

    # Price crosses over Kijun Sen
    cross_kijun = event.crossabove_scan(test_data['Close'], df_ichi_kijun)
    print 'Price X Kijun:(ETF)' + str(cross_kijun)

    # TTM squeeze firing
    ttm_cross_u = event.crossabove_scan(df_bb_u, df_kch_u)
    print 'TTM Upper Cross: ' + str(ttm_cross_u)

    # MACD Histogram Rising
    macd_rising = event.rising_scan(test_data['Close'])
    print 'MACD Histogram Rising:' + str(macd_rising)

    # Explosive EMA power
    # EMA 21 is below EMA 89
    # Price crosses above EMA 8

    ema21b89 = event.is_above_scan(df_ema21, df_ema8)
    pricex8 = event.crossabove_scan(test_data['Close'], df_ema8, ema21b89)

    print 'Explosive Power: ' + str(pricex8)

    current_time = datetime.datetime.now().time()
    print 'End time:' + str(current_time)

if __name__ == '__main__':
    main()
