import pandas as pd
import math
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import copy
import numpy as np


'''
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep
import csv
'''
#import sys
from datetime import datetime, date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
'''
def maxloss(date, window_size, df_equity):
    for date_idx in range(df_equity.index):
        if df_equity.index[date_idx+window_size] <= df_equity.index[-1]:
'''

# Return a data frame of daily equity changes        
def portvalchanges(df_equity):
    df_port_val_changes = copy.deepcopy(df_equity)
    df_port_val_changes = df_port_val_changes * 0
    
    for date_idx in range(0,len(df_equity.index)):
        if df_equity.index[date_idx] > df_equity.index[0]:
            df_port_val_changes[0].ix[df_equity.index[date_idx]] = df_equity[0].ix[df_equity.index[date_idx]]-df_equity[0].ix[df_equity.index[date_idx-1]]
    return df_port_val_changes

def maxdrawdown(df_equity):
    df_rollsum = copy.deepcopy(df_equity)
    df_rollsum = df_rollsum * 0
    
    #windows = [2,4,8,16,32]
    windows = np.arange(2,51)
    columns =['rollsum']
    index = windows
    
    df_rsum = pd.DataFrame(index=index,columns=columns)
    df_rsum = df_rsum.fillna(0)

    
    for window_size in windows:
        df_rollsum[0] = pd.rolling_sum(df_equity[0],window_size)
        df_rsum['rollsum'].ix[window_size] = df_rollsum[0].min(axis=0)
    #df_equity.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_equity.csv')
    df_rsum.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_rsum.csv')
    df_rollsum.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_rollsum.csv')
    return df_rsum.min(axis=0)

def plot_stock(quotes):
    fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.2)
    #ax.xaxis.set_major_locator(mondays)
    #ax.xaxis.set_minor_locator(alldays)
    #ax.xaxis.set_major_formatter(weekFormatter)
    #ax.xaxis.set_minor_formatter(dayFormatter)

    #plot_day_summary(ax, quotes, ticksize=3)
    candlestick(ax, quotes, width=0.6)

    ax.xaxis_date()
    ax.autoscale_view()
    plt.setp( plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    plt.savefig('stock.pdf', format='pdf')
    #plt.show()

def plots(index,series1, series2, series3, series4, file_name):
    path = './plots/'+file_name+'_'+str(date.today())+'.pdf'
    #pp = PdfPages('./plots/plots.pdf')
    pp = PdfPages(path)
    tot_symbols = len(series1.columns)
    fig = plt.figure()
    
    d = pp.infodict()
    d['Title'] = 'Watchlist Chart Book'
    d['Author'] = u'Rahul Kumar'
    d['Subject'] = 'Watchlist Chart Book'
    d['Keywords'] = 'Watchlist Charts'
    #d['CreationDate'] = dt.datetime(2009, 11, 13)
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()

    for subplot in range(1,tot_symbols+1):
        #print series1.columns[subplot-1]
        #ax = fig.add_subplot(tot_symbols,1,subplot)
        plt.plot(index, series1[series1.columns[subplot-1]])  # $SPX 50 days
        plt.plot(index, series2[series2.columns[subplot-1]])  # XOM 50 days
        plt.plot(index, series3[series3.columns[subplot-1]])  # XOM 50 days
        plt.plot(index, series4[series4.columns[subplot-1]])  # XOM 50 days
        #plt.axhline(y=0, color='r')
        plt.legend([series1.columns[subplot-1]], loc='best')
        plt.ylabel('Daily Returns',size='xx-small')
        plt.xlabel(series1.columns[subplot-1],size='xx-small')
        plt.xticks(size='xx-small')
        plt.yticks(size='xx-small')
        plt.savefig(pp, format='pdf')
        plt.close()
    pp.close()
    

    

    
def plot(index,series1, series2, series3, series4):
    #fig = plt.figure()
    plt.clf()
    plt.plot(index, series1)  # $SPX 50 days
    plt.plot(index, series2)  # XOM 50 days
    plt.plot(index, series3)  # XOM 50 days
    plt.plot(index, series4)  # XOM 50 days
    #plt.axhline(y=0, color='r')
    plt.legend(['Portfolio', 'SPX '], loc='best')
    plt.ylabel('Daily Returns',size='xx-small')
    plt.xlabel('Date',size='xx-small')
    plt.xticks(size='xx-small')
    plt.yticks(size='xx-small')
    plt.savefig('channel.pdf', format='pdf')

    
    
def analyze(analyzefile):
    print 'Inside Analyze'
    file_path = 'C:\\Users\\owner\\Documents\\Software\\Python\\Quant\\Examples\\ZeroSum Strategy Suite\\'
    #analyze_file = sys.argv[1]
    #analyze_file = 'values.csv'
    analyze_file = analyzefile
    input_file = file_path+analyze_file



    port_value = pd.read_csv(input_file, sep=',',index_col = 0, header=0,names=['PortVal'])

    port_daily_ret = pd.DataFrame(range(len(port_value)),index=port_value.index, dtype='float')


    startdate = datetime.strptime(port_value.index[0],'%Y-%m-%d %H:%M:%S')
    enddate = datetime.strptime(port_value.index[len(port_value)-1],'%Y-%m-%d %H:%M:%S')

    #benchmark = sys.argv[2]
    benchmark = ['$SPX']
    #benchmark = ['SPY']
    #benchmark = bench
    
    #d_data = data
        
    # Start and End date of the charts
    dt_start = dt.datetime(startdate.year, startdate.month, startdate.day)
    dt_end = dt.datetime(enddate.year, enddate.month, enddate.day+1)

    # We need closing prices so the timestamp should be hours=16.
    dt_timeofday = dt.timedelta(hours=16)

    # Get a list of trading days between the start and the end.
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)

    # Creating an object of the dataaccess class with Yahoo as the source.
    #c_dataobj = da.DataAccess('Yahoo',verbose=True,cachestalltime = 0)
    c_dataobj = da.DataAccess('Yahoo')

    # Keys to be read from the data, it is good to read everything in one go.
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']

    # Reading the data, now d_data is a dictionary with the keys above.
    # Timestamps and symbols are the ones that were specified before.
    ldf_data = c_dataobj.get_data(ldt_timestamps, benchmark, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    # Filling the data for NAN
    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)    

    df_close = d_data['close']

    #df_close = benchdata
    bench_daily_ret = pd.DataFrame(range(len(df_close)),index=df_close.index, dtype='float')
    bench_val = pd.DataFrame(range(len(port_value)),index=port_value.index)
    bench_init_investment = port_value['PortVal'].ix[0]
    bench_val[0].ix[0] = bench_init_investment
    

    # Portfolio Daily Returns
    for row_idx in range(0,len(ldt_timestamps)):
        #Start calculating daily return on day 2
        if row_idx > 0:
            port_daily_ret[0].ix[row_idx] = (float(port_value['PortVal'].ix[row_idx])/float(port_value['PortVal'].ix[row_idx-1]))-1
            
    # Benchmark Daily Returns
    for row_idx in range(0,len(ldt_timestamps)):
        #Start calculating daily return on day 2
        if row_idx > 0:
            bench_daily_ret[0].ix[row_idx] = (float(df_close[benchmark].ix[row_idx])/float(df_close[benchmark].ix[row_idx-1]))-1

    #Bench Value
    for row_idx in range(1,len(ldt_timestamps)):
        bench_val[0].ix[row_idx] = bench_val[0].ix[row_idx-1] * (1+bench_daily_ret[0].ix[row_idx])
            
            
    avg_port_daily_ret = port_daily_ret.mean(axis=0)
    avg_bench_daily_ret = bench_daily_ret.mean(axis=0)

    port_vol = port_daily_ret.std(axis=0)
    bench_vol = bench_daily_ret.std(axis=0)

    port_sharpe = math.sqrt(252)*(avg_port_daily_ret/port_vol)
    bench_sharpe = math.sqrt(252)*(avg_bench_daily_ret/bench_vol)
        
    port_cum_ret = float(port_value['PortVal'].ix[len(ldt_timestamps)-1])/float(port_value['PortVal'].ix[0])
    bench_cum_ret = df_close[benchmark].ix[len(ldt_timestamps)-1]/df_close[benchmark].ix[0]
    
    # Plotting the plot of daily returns
    plt.clf()
    plt.plot(ldt_timestamps[0:], port_value['PortVal'])  # $SPX 50 days
    plt.plot(ldt_timestamps[0:], bench_val[0])  # XOM 50 days
    #plt.axhline(y=0, color='r')
    plt.legend(['Portfolio', 'SPX '], loc='best')
    plt.ylabel('Daily Returns',size='xx-small')
    plt.xlabel('Date',size='xx-small')
    plt.xticks(size='xx-small')
    plt.yticks(size='xx-small')
    plt.savefig('rets.pdf', format='pdf')

    print 'Sharpe ratio of fund:'+str(port_sharpe)
    print 'Sharpe ratio of benchmark:'+str(bench_sharpe)

    print 'Total Return of fund:'+str(port_cum_ret)
    print 'Total Return of benchmark:'+str(bench_cum_ret)

    print 'Standard Deviation of fund:'+str(port_vol)
    print 'Standard Deviation of benchmark:'+str(bench_vol)

    print 'Average Daily Return of fund:'+str(avg_port_daily_ret)
    print 'Average Daily Return of benchmark:'+str(avg_bench_daily_ret)
