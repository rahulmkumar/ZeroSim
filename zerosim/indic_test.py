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
    ibdlow = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBDLOW')

    # Technology Stocks
    yahoo_stocks = data.SymbolDb()
    tech = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Volume='1000000', Sector='Technology')
    biotech1 = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Volume='1000000', Industry='Bio')
    pricegt_ten = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Mcap=[500,5000], Pricegt=10)
    pricelt_ten = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Mcap=[500,5000], Pricelt=10)

    options = list(set(ibd50 + biotech + ETFOptions + tech + biotech1))

    blueprint = list(set(pricegt_ten + pricelt_ten + ibdlow))

    #blueprint = ibdlow

    # Get data for watchlist

    current_time = datetime.datetime.now().time()
    print 'Data download start time:' + str(current_time)

    dat = data.MarketData()
    test_data = dat.get_yahoo_data(options, '02/01/2014', '07/31/2015')
    blueprint_data = dat.get_yahoo_data(blueprint, '02/01/2014', '07/31/2015')
    test_data['Close'] = test_data['Close'].fillna(method='ffill')
    test_data['Open'] = test_data['Open'].fillna(method='ffill')
    test_data['High'] = test_data['High'].fillna(method='ffill')
    test_data['Low'] = test_data['Low'].fillna(method='ffill')

    #test_data['Close'].ffill()
    #df_test = test_data['Close'].drop(test_data['Close'].index[79])
    #test_data['Close'].to_csv('test_data.csv')
    #df_test.to_csv('df_test.csv')

    current_time = datetime.datetime.now().time()
    print 'Data download end time:' + str(current_time)

    ind = ta.Indicators()

    # Bollinger Bands
    df_bb_u, df_bb_m, df_bb_l = ind.bb(options, test_data['Close'], 20, 2, 2)

    # Fibonacci EMA's and Waves
    df_ema8 = ind.ema(options, test_data['Close'], 8)
    df_ema21 = ind.ema(options, test_data['Close'], 21)
    df_ema34 = ind.ema(options, test_data['Close'], 34)
    df_ema55 = ind.ema(options, test_data['Close'], 55)
    df_ema89 = ind.ema(options, test_data['Close'], 89)

    df_waveaa, df_waveab = ind.wavea(options, df_ema8, df_ema21, df_ema34)
    df_waveba, df_wavebb = ind.wavea(options, df_ema21, df_ema34, df_ema55)
    df_waveca, df_wavecb = ind.wavea(options, df_ema34, df_ema55, df_ema89)

    df_ema40 = ind.ema(options, test_data['Close'], 40)
    df_ema105 = ind.ema(options, test_data['Close'], 105)
    df_ema170 = ind.ema(options, test_data['Close'], 170)
    df_ema275 = ind.ema(options, test_data['Close'], 275)
    df_ema445 = ind.ema(options, test_data['Close'], 445)

    # Keltner Channels
    df_kelt_u, df_kelt_m, df_kelt_l = ind.keltner(options, test_data['High'], test_data['Low'], test_data['Close'], 20, 20, 2)

    # TTM Squeeze Test
    df_bb_ma, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l = ind.ttm_squeeze(blueprint, test_data['High'], test_data['Low'], test_data['Close'], 21, 2, 21, 21, 1.5)

    # Ichimoku Test
    df_ichi_tenkan, df_ichi_kijun = ind.ichimoku(options, test_data['High'], test_data['Low'])

    # Momentum
    df_mom = ind.mom(options, test_data['Close'], 12)

    # ADX
    df_adx = ind.adx(options, test_data['High'], test_data['Low'], test_data['Close'], 14)

    #MACD
    df_macd, df_macdsig, df_macdhist = ind.macd(options, test_data['Close'], 12, 26, 9)

    #Stochastic RSI

    df_fastk, df_fastd = ind.stochastic_rsi(options, test_data['Close'], 5, 3)
    df_fastd.to_csv('df_fastd.csv')

    # Open Text File
    file_tmstmp = str(datetime.datetime.now().month) + str(datetime.datetime.now().day) + str(datetime.datetime.now().year)
    file_name = 'options_scan_results_' + file_tmstmp + '.txt'
    f = open(file_name, 'w+')

    f.write('Total Stocks:' + str(len(options)) + '\n')
    f.write('----------------------------------------------\n')

    # Event Scanner
    event = ta.Events()
    cross_sym = event.crossabove_scan(df_ema8, df_ema21)
    f.write('EMA8 X EMA21:'+ str(cross_sym)+'\n')
    f.write('----------------------------------------------\n')

    # Ichimoku Cross
    cross_ichimoku = event.crossabove_scan(df_ichi_tenkan, df_ichi_kijun)
    f.write('Tenkan X Kijun(ETF):' + str(cross_ichimoku) + '\n')
    f.write('----------------------------------------------\n')

    # Price crosses over Kijun Sen
    cross_kijun = event.crossabove_scan(test_data['Close'], df_ichi_kijun)
    f.write('Price X Kijun:(ETF)' + str(cross_kijun) + '\n')
    f.write('----------------------------------------------\n')

    # TTM squeeze firing
    ttm_cross_u = event.crossabove_scan(df_bb_u, df_kch_u,options)
    f.write('TTM Upper Cross: ' + str(ttm_cross_u) + '\n')
    f.write('----------------------------------------------\n')

    # MACD Histogram Rising
    macd_rising = event.rising_scan(df_macdhist)
    f.write('MACD Histogram Rising:' + str(macd_rising) + '\n')
    f.write('----------------------------------------------\n')

    # Explosive EMA power
    # EMA 21 is below EMA 89
    # Price crosses above EMA 8

    ema21b89 = event.is_below_scan(df_ema21, df_ema89, ETFOptions)
    pricex8 = event.crossabove_scan(test_data['Close'], df_ema8, ema21b89)

    f.write('Explosive Power: ' + str(pricex8) + '\n')
    f.write('----------------------------------------------\n')

    # Turning up scan
    macd_turn_up = event.turningup_scan(df_macdhist)
    f.write('MACD Histogram Turn Up:' + str(macd_turn_up) + '\n')
    f.write('----------------------------------------------\n')

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
    f.write('----------------------------------------------\n')

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
    f.write('----------------------------------------------\n')

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
    f.write('----------------------------------------------\n')

    # Oversold Stochastic
    stoch_sold = event.is_below_N_scan(df_fastk, 20)
    f.write(('Stochastic below 20:') + str(stoch_sold) + '\n')
    f.write('----------------------------------------------\n')

    f.close()

    '''
    Blueprint Scanning
    '''
    current_time = datetime.datetime.now().time()
    print 'Blueprint processing start time:' + str(current_time)

    sma8 = ind.sma(blueprint, blueprint_data['Close'], 8)
    sma50 = ind.sma(blueprint, blueprint_data['Close'], 50)

    bp_file_name = 'blueprint_scan_results_' + file_tmstmp + '.txt'
    f = open(bp_file_name, 'w+')

    f.write('Total Stocks:' + str(len(blueprint)) + '\n')
    f.write('----------------------------------------------\n')

    sma_cross = event.crossabove_scan(sma8, sma50, blueprint)
    f.write('SMA 8 X SMA 50:' + str(sma_cross) + '\n')
    f.write('----------------------------------------------\n')

    # TTM squeeze firing
    ttm_cross_u_bp = event.crossabove_scan(df_bb_u, df_kch_u, blueprint)
    f.write('TTM Upper Cross: ' + str(ttm_cross_u_bp) + '\n')
    f.write('----------------------------------------------\n')

    f.close()

    current_time = datetime.datetime.now().time()
    print 'End time:' + str(current_time)

if __name__ == '__main__':
    main()
