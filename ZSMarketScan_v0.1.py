'''
This version builds upon V0.2 by eliminating multiple data source calls
Instead, data frames are passed between different functions
Multiple functions and module files have also been created for better code reuse
'''
#Libraries
import QSTK.qstkstudy.EventProfiler as ep
import sys
import time
#Project modules
import events
import analysis
import marketdata
import simulator
import indicators
import copy

'''
Command line execution:
python zsmarketsim.py <symbols.csv> <benchmark> <start_time> <end_time> <starting equity> <entry strategy> <exit strategy> <entry filter> <exit filter> <position sizing strategy>
python zsmarketscan_v0.1.py sp5002012 01/01/2013 12/31/2013
'''    
def main():
    #print "Pandas Version", pd.__version__
    
    symbol_file = sys.argv[1]
    startdate = sys.argv[2]
    enddate = sys.argv[3]
    benchmark = sys.argv[4]
    scan = sys.argv[5]
    

    '''
    starting_equity = sys.argv[4]
    benchmark = sys.argv[5]
    benchmark = sys.argv[5]
    entry_strategy = sys.argv[6]
    exit_strategy = sys.argv[7]
    entry_filter = sys.argv[8]
    exit_filter = sys.argv[9]
    pos_size_strategy = sys.argv[10]
    '''
    # Timing code
    #start_time_getdata = time.time()
    #print 'Getdata took:', time.time() - start_time_getdata

    # Get Market data from Yahoo
    #d_data, ls_symbols = marketdata.get_data(startdate, enddate,symbol_file,benchmark)
    
    # Get Market data from SQLite database (previously loaded from Yahoo
    df_open,df_high, df_low,df_close, ls_symbols = marketdata.get_sqlitedb_data_all(startdate, enddate, symbol_file, benchmark)
    #df_prices, ls_symbols = marketdata.get_sqlitedb_data(startdate, enddate, symbol_file, benchmark, 'Close')
    #df_high, ls_symbols = marketdata.get_sqlitedb_data(startdate, enddate, symbol_file, benchmark, 'High')
    #df_low, ls_symbols = marketdata.get_sqlitedb_data(startdate, enddate, symbol_file, benchmark, 'Low')
    #df_prices = d_data['close']
    #df_prices = d_data['close']
    #df_prices.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_prices1.csv')
    #df_prices.sort_index(axis=1,ascending=False)
    df_close.sort_index(inplace=True)
    #/home/rahul/workspace/ZSStrategySuite/df_prices.csv
    
    
    #df_sma = indicators.sma(df_prices,50)
    #df_uch, df_lch = indicators.channel(df_prices,50)
    #analysis.plot(df_uch.index,df_prices['AAPL'],df_uch['AAPL'],df_lch['AAPL'],df_sma['AAPL'])
    #analysis.plots(df_uch.index,df_prices,df_uch,df_lch,df_sma,'channel_sma')
     
    
    #plt = analysis.plot(df_uch.index,df_prices['XOM'],df_uch['XOM'],df_lch['XOM'],df_sma['XOM'])

    
    # Find Events and create Event profile
    #df_bb_events = events.find_bb_events(ls_symbols, d_data, benchmark)
    
    ##############################
    # Scan for lifetime highs
    ##############################
    '''
    df_lifetime_high_syms = events.lifetimehigh(df_close,1)
    #print len(df_lifetime_high_syms.columns)
    df_lifetime_high_syms.to_csv('./debug/df_lifetime_high_'+symbol_file+'_syms.csv')
    df_close.to_csv('./debug/df_prices.csv')
    
    df_criteria_sym = df_close.copy()
    df_criteria_sym.to_csv('./debug/df_criteria_sym.csv')
    
    df_lifetime_prices = events.event_prices(df_criteria_sym, df_lifetime_high_syms)
    #print len(df_lifetime_prices.columns) 
    df_lifetime_prices.to_csv('./debug/df_lifetime_prices.csv')
    
    
    df_sma_filter = indicators.sma(df_lifetime_prices,50)
    df_uch_filter, df_lch_filter = indicators.channel(df_lifetime_prices, 50)
    #print len(df_prices.columns)
    #print len(df_sma_filter.columns)
    #print len(df_uch_filter.columns)
    #print len(df_lch_filter.columns)
    '''
    #analysis.plots(df_close.index,df_lifetime_prices, df_sma_filter, df_uch_filter, df_lch_filter, 'lifetime_highs_'+symbol_file)
    
    ##############################
    # Scan for new N day highs
    ##############################
    
    
    #df_u_ch.to_csv('./debug/df_u_ch.csv')
    #df_prices.to_csv('./debug/df_prices.csv')
    '''
    df_u_ch, df_l_ch = indicators.channel(df_close,50)
    
    df_ch_cross = events.crossabove_df(df_close,df_u_ch)
    df_ch_cross.to_csv('./debug/df_50_day_history_high_'+symbol_file+'_syms.csv')
    df_ch_cross.sort_index(ascending=False,inplace=True)

    df_ch_cross_sym = events.event_prices(df_close,df_ch_cross)
    df_ch_u_channel = events.event_prices(df_close, df_u_ch)
    df_ch_l_channel = events.event_prices(df_close, df_l_ch)
    
    df_ch_cross_sym.to_csv('./debug/df_50_day_high_'+symbol_file+'_syms.csv')
    
    '''
    
    #analysis.plots(df_close.index,df_ch_cross_sym, df_ch_cross_sym, df_ch_u_channel, df_ch_l_channel, '50_day_highs_'+symbol_file)
    
    
    #df_atr = indicators.atr(df_close,df_high,df_low,20,1)
    
    #df_sto = indicators.stochastic(df_close, df_high, df_low, 20)
    #df_ema = indicators.ema(df_close,20)
    #df_macd, df_signal = indicators.macd(df_close,12,26,9)
    #df_kch_m, df_kch_u, df_kch_l = indicators.keltner(df_close,df_high,df_low,20,10,2)
    #df_mae, df_mau, df_mal = indicators.mae(df_close,20,0.025)
    
    #df_adx = indicators.adx(df_close,df_high,df_low,20)
    #df_mkt_stats = events.SMA_level_stats(df_close, 50, 200)
    #EMA Test
    '''
    df_ema10 = indicators.ema(df_close,10)
    df_ema30 = indicators.ema(df_close,30)
    
    df_bet = events.is_between_df(df_close, df_ema30, df_ema10)
    df_bet.to_csv('./debug/df_bet.csv')
    '''
    ##########################################################

    '''
    TTM Squeeze
    '''
    if scan =='TTM':
        #df_bb_20_ma, df_bb_20_u, df_bb_20_l  = indicators.bb(df_close,20,2)
        #df_kc_20_15_m, df_kc_20_15_u, df_kc_20_15_l = indicators.keltner(df_close,df_high,df_low,20,1.5,1)
        
        df_bb_20_ma, df_bb_20_u, df_bb_20_l, df_kc_20_15_m, df_kc_20_15_u, df_kc_20_15_l = indicators.TTM_squeeze(df_close,df_high, df_low,20,2,20,1.5,1)
        
        df_squeeze_upper = events.is_below_df(df_bb_20_u,df_kc_20_15_u)
        df_squeeze_lower = events.is_above_df(df_bb_20_l,df_kc_20_15_l)
        
        df_bb_20_u.to_csv('./debug/df_bb_20_u.csv')
        df_bb_20_l.to_csv('./debug/df_bb_20_l.csv')
        df_squeeze_upper.to_csv('./debug/df_squeeze_upper.csv')
        df_squeeze_lower.to_csv('./debug/df_squeeze_lower.csv')
    
    '''
    ICHIMOKU: Tenkan Sen, Kijun Sen
    Tenkan Sen: 9 periods
    Kijun Sen: 26 periods
    Senkou Span A: (Tenkan Sen + Kijun Sen)/2 shifted forward 26 periods
    Senkou Span B: (Lowest Low + Highest High)/2 of 52 periods shifted forward 26 periods 
    '''
    if scan == 'ICHIMOKU':
        df_tenkan, df_kijun = indicators.ichimoku(df_close, df_high, df_low)
        df_tenkan.to_csv('./debug/df_tenkan.csv')
        df_kijun.to_csv('./debug/df_kijun.csv')
    
    
    
    
    
    
    #analysis.plots(df_close.index,df_atr, df_atr, df_ema, df_sto, '20_day_'+symbol_file)
    #analysis.plots(df_close.index,df_close, df_macd, df_ema, df_signal, 'MACD_'+symbol_file)

    #analysis.plot_stock(d_data['XOM'])
    
    #marketdata.combine_tickers()
    

        
if __name__ == '__main__':
    main()    
