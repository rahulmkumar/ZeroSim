'''
This version builds upon V0.2 by eliminating multiple data source calls
Instead, data frames are passed between different functions
Multiple functions and module files have also been created for better code reuse
'''
#Libraries
import QSTK.qstkstudy.EventProfiler as ep
import sys

#Project modules
import events
import analysis
import marketdata
import simulator
import indicators

'''
Command line execution:
python zsmarketsim.py <symbols.csv> <benchmark> <start_time> <end_time> <starting equity> <entry strategy> <exit strategy> <entry filter> <exit filter> <position sizing strategy>
python zsmarketsim.py sp5002012 01/01/2008 12/31/2009 100000 SPY
'''    
def main():
    #print "Pandas Version", pd.__version__
    
    symbol_file = sys.argv[1]
    startdate = sys.argv[2]
    enddate = sys.argv[3]
    starting_equity = sys.argv[4]
    benchmark = sys.argv[5]
    '''
    benchmark = sys.argv[5]
    entry_strategy = sys.argv[6]
    exit_strategy = sys.argv[7]
    entry_filter = sys.argv[8]
    exit_filter = sys.argv[9]
    pos_size_strategy = sys.argv[10]
    '''

    # Get Market data from Yahoo files
    #d_data, ls_symbols = marketdata.get_data(startdate, enddate,symbol_file,benchmark)
    #df_prices = d_data['close']
    
    # Get Market data from SQLite database (previously loaded from Yahoo
    df_prices, ls_symbols = marketdata.get_sqlitedb_data(startdate, enddate, symbol_file, benchmark, 'Close')
    
    
        
    #df_sma = indicators.sma(df_prices,50)
    
    #df_uch, df_lch = indicators.channel(df_prices,50)
    
    #analysis.plot(df_uch.index,df_prices['AAPL'],df_uch['AAPL'],df_lch['AAPL'],df_sma['AAPL'])
    
    # Find Events and create Event profile
    df_bb_events = events.find_bb_events(ls_symbols, df_prices, benchmark)
    #Generate an Event Profile
    simulator.marketsim(100000, 'mydata.csv', 'portval.csv',df_prices)
    
    #Analyze the simulation Results
    analysis.analyze('portval.csv')

'''
    ep.eventprofiler(df_bb_events, d_data, i_lookback=20, i_lookforward=20,
                s_filename='BBStudy_2012.pdf', b_market_neutral=True, b_errorbars=True,
                s_market_sym=benchmark)
'''
    #Perform simulation
        
if __name__ == '__main__':
    main()    