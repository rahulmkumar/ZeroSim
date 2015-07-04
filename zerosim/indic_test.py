__author__ = 'rahul'
import data
import ta
import datetime



def main():

    current_time = datetime.datetime.now().time()
    print 'Start time:' + str(current_time)

    # Refresh Watchlist
    watch_list = data.WatchlistDb()
    updated_dict = watch_list.get_watchlists_csv('../symbols/watchlistdb.csv')

    conn = watch_list.open_watchlistdb('data/watchlistdb.db')
    watch_list.insert_multiple_watchlists(conn, updated_dict)

    # Return watchlist
    ibd50 = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')
    biotech = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'Biotech')
    ETFOptions = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'ETFOptions')

    # Technology Stocks
    yahoo_stocks = data.SymbolDb()
    tech = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Volume='1000000', Sector='Technology')

    wlist = list(set(ibd50 + biotech + ETFOptions + tech))

    # Get data for watchlist
    dat = data.MarketData()
    test_data = dat.get_yahoo_data(wlist, '02/01/2014', '07/03/2015')

    ind = ta.Indicators()

    # Bollinger Bands
    df_bb_u, df_bb_m, df_bb_l = ind.bb(wlist, test_data['Close'], 20, 2, 2)

    # Fibonacci EMA's and Waves
    df_ema8 = ind.ema(wlist, test_data['Close'], 8)
    df_ema21 = ind.ema(wlist, test_data['Close'], 21)
    df_ema34 = ind.ema(wlist, test_data['Close'], 34)
    df_ema55 = ind.ema(wlist, test_data['Close'], 55)
    df_ema89 = ind.ema(wlist, test_data['Close'], 89)

    df_waveaa, df_waveab = ind.wavea(wlist, df_ema8, df_ema21, df_ema34)
    df_waveba, df_wavebb = ind.wavea(wlist, df_ema21, df_ema34, df_ema55)
    df_waveca, df_wavecb = ind.wavea(wlist, df_ema34, df_ema55, df_ema89)

    df_ema40 = ind.ema(wlist, test_data['Close'], 40)
    df_ema105 = ind.ema(wlist, test_data['Close'], 105)
    df_ema170 = ind.ema(wlist, test_data['Close'], 170)
    df_ema275 = ind.ema(wlist, test_data['Close'], 275)
    df_ema445 = ind.ema(wlist, test_data['Close'], 445)

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

    #MACD
    df_macd, df_macdsig, df_macdhist = ind.macd(wlist, test_data['Close'], 12, 26, 9)

    # Open Text File
    file_tmstmp = str(datetime.datetime.now().month) + str(datetime.datetime.now().day) + str(datetime.datetime.now().year)
    file_name = 'scan_results_' + file_tmstmp + '.txt'
    f = open(file_name, 'w+')

    # Event Scanner
    event = ta.Events()
    cross_sym = event.crossabove_scan(df_ema8, df_ema21)
    f.write('EMA8 X EMA21:'+ str(cross_sym)+'\n')

    # Ichimoku Cross
    cross_ichimoku = event.crossabove_scan(df_ichi_tenkan, df_ichi_kijun)
    f.write('Tenkan X Kijun(ETF):' + str(cross_ichimoku) + '\n')

    # Price crosses over Kijun Sen
    cross_kijun = event.crossabove_scan(test_data['Close'], df_ichi_kijun)
    f.write('Price X Kijun:(ETF)' + str(cross_kijun) + '\n')

    # TTM squeeze firing
    ttm_cross_u = event.crossabove_scan(df_bb_u, df_kch_u)
    f.write('TTM Upper Cross: ' + str(ttm_cross_u) + '\n')

    # MACD Histogram Rising
    macd_rising = event.rising_scan(df_macdhist)
    f.write('MACD Histogram Rising:' + str(macd_rising) + '\n')

    # Explosive EMA power
    # EMA 21 is below EMA 89
    # Price crosses above EMA 8

    ema21b89 = event.is_below_scan(df_ema21, df_ema89, ETFOptions)
    pricex8 = event.crossabove_scan(test_data['Close'], df_ema8, ema21b89)

    f.write('Explosive Power: ' + str(pricex8) + '\n')

    # Turning up scan
    macd_turn_up = event.turningup_scan(df_macdhist)
    f.write('MACD Histogram Turn Up:' + str(macd_turn_up) + '\n')

    # Price crosses EMA 8
    price_ema8 = event.crossabove_scan(test_data['Close'], df_ema8)
    f.write('Price crosses EMA 8:' + str(price_ema8) + '\n')

    # Price crosses EMA 21
    price_ema21 = event.crossabove_scan(test_data['Close'], df_ema21)
    f.write('Price crosses EMA 21:' + str(price_ema21) + '\n')

    # Price crosses EMA 34
    price_ema34 = event.crossabove_scan(test_data['Close'], df_ema34)
    f.write('Price crosses EMA 34:' + str(price_ema34) + '\n')

    # Price crosses EMA 55
    price_ema55 = event.crossabove_scan(test_data['Close'], df_ema55)
    f.write('Price crosses EMA 55:' + str(price_ema55) + '\n')

    # Price crosses EMA 89
    price_ema89 = event.crossabove_scan(test_data['Close'], df_ema89)
    f.write('Price crosses EMA 89:' + str(price_ema89) + '\n')

    # Price crosses EMA 40
    price_ema40 = event.crossabove_scan(test_data['Close'], df_ema40)
    f.write('Price crosses EMA 40:' + str(price_ema40) + '\n')

    # Price crosses EMA 105
    price_ema105 = event.crossabove_scan(test_data['Close'], df_ema105)
    f.write('Price crosses EMA 105:' + str(price_ema105) + '\n')

    # Price crosses EMA 170
    price_ema170 = event.crossabove_scan(test_data['Close'], df_ema170)
    f.write('Price crosses EMA 170:' + str(price_ema170) + '\n')

    # Price crosses EMA 275
    price_ema275 = event.crossabove_scan(test_data['Close'], df_ema275)
    f.write('Price crosses EMA 275:' + str(price_ema275) + '\n')

    # Price crosses EMA 445
    price_ema445 = event.crossabove_scan(test_data['Close'], df_ema445)
    f.write('Price crosses EMA 445:' + str(price_ema445) + '\n')

    # Wave A Rising
    waveaa_rising = event.rising_scan(df_waveaa)
    f.write(('Wave Aa Rising:') + str(waveaa_rising) + '\n')

    waveab_rising = event.rising_scan(df_waveab)
    f.write(('Wave Ab Rising:') + str(waveab_rising) + '\n')

    # Wave A Turning Up
    waveaa_turnup = event.turningup_scan(df_waveaa)
    f.write(('Wave Aa Turn Up:') + str(waveaa_turnup) + '\n')

    waveab_turnup = event.turningup_scan(df_waveab)
    f.write(('Wave Ab Turn Up:') + str(waveab_turnup) + '\n')

    f.close()

    current_time = datetime.datetime.now().time()
    print 'End time:' + str(current_time)

if __name__ == '__main__':
    main()
