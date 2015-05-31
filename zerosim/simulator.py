import pandas as pd
import numpy as np
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
from datetime import datetime


import analysis

def marketsim(startequity, orderfile, outputfilename,data):
    print 'Inside marketsim'
    #Starting cash
    start_cash = startequity
    
    #File Path
    file_path = 'C:\\Users\\owner\\Documents\\Software\\Python\\Quant\\Examples\\ZeroSum Strategy Suite\\'
    file_path ='/home/rahul/workspace/ZSSuite/'
    #Orders Files
    order_file = orderfile
    input_file = file_path+order_file
    #input_file = os.path.join(order_file_path, order_file)
            
    #Output File
    #output_file = output_path+sys.argv[3]
    #output_file = os.path.join(order_file_path, sys.argv[3])
    #output_file = file_path+sys.argv[3]
    output_file = file_path+outputfilename
    
    df_data = data
    
    #start_cash = 1000000

    # Reading the csv file.
    na_orders = np.loadtxt(input_file, dtype={'names': ('year', 'month', 'day','sym','bh','qty'),'formats': ('i4','i4','i4','S5', 'S5', 'i4')},delimiter=',', skiprows=0)

    symbols = []
    date = []
    for port in na_orders:
        date.append(datetime.strptime((str(port[0])+'/'+str(port[1])+'/'+str(port[2])),'%Y/%m/%d'))
        symbols.append(port[3])
            
    #orders = np.zeros((len(na_orders),6),dtype=('i4,i4,i4,a5,a5,i4'))
        
    #na_orders1 = np.loadtxt(orderfile, orders, dtype={'names': ('year', 'month', 'day','sym','bh','qty'),'formats': ('i4','i4','i4','S5', 'S5', 'i4')},delimiter=',', skiprows=0)
        
            
    startdate = min(date)
    enddate = max(date)
        
    sym_list = set(symbols)

    # Start and End date of the charts
    dt_start = dt.datetime(startdate.year, startdate.month, startdate.day)
    dt_end = dt.datetime(enddate.year, enddate.month, enddate.day+1)

    # We need closing prices so the timestamp should be hours=16.
    dt_timeofday = dt.timedelta(hours=16)

    # Get a list of trading days between the start and the end.
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)

    # Creating an object of the dataaccess class with Yahoo as the source.
    #c_dataobj = da.DataAccess('Yahoo',verbose=True,cachestalltime = 0)
    #c_dataobj = da.DataAccess('Yahoo',verbose=True)

    # Keys to be read from the data, it is good to read everything in one go.
    #ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']

    # Reading the data, now d_data is a dictionary with the keys above.
    # Timestamps and symbols are the ones that were specified before.
    #ldf_data = c_dataobj.get_data(ldt_timestamps, sym_list, ls_keys)
    #d_data = dict(zip(ls_keys, ldf_data))

    # Filling the data for NAN
    #for s_key in ls_keys:
    #    d_data[s_key] = d_data[s_key].fillna(method='ffill')
    #    d_data[s_key] = d_data[s_key].fillna(method='bfill')
    #    d_data[s_key] = d_data[s_key].fillna(1.0)

    order_dates = []
        
    for row in range(0,len(na_orders)):
        order_dates.append(datetime(na_orders[row][0],na_orders[row][1],na_orders[row][2],16))

    # Orders DataFrame 
    df_orders = pd.DataFrame(na_orders, index=order_dates)
    df_orders.sort_index(axis=0,ascending=True, inplace=True)

    #Close Prices DataFrame
    #df_close = d_data['close']
    df_close = df_data

    #Cash DataFrame
    df_cash = pd.DataFrame(range(len(ldt_timestamps)),index=ldt_timestamps)

    for cash_dt in ldt_timestamps:
        df_cash.ix[cash_dt] = start_cash

    #Ownership DataFrame
    #df_own = pd.DataFrame(range(len(ldt_timestamps)), index=ldt_timestamps)
    # Creating an empty dataframe
    df_own = copy.deepcopy(df_close)
    df_own = df_own * 0


    #Value DataFrame
    #df_Hold_Value = pd.DataFrame(range(len(ldt_timestamps)), index=ldt_timestamps)
    df_hold_value = copy.deepcopy(df_close)
    df_hold_value = df_hold_value * 0.0

    # Total Holdings Value
    #df_total_hold_value = pd.DataFrame(range(len(ldt_timestamps)),index=ldt_timestamps)
    df_total_hold_value = copy.deepcopy(df_cash)
    df_total_hold_value = df_total_hold_value * 0

    #Total Portfolio Value
    df_port_value = copy.deepcopy(df_cash)
    df_port_value = df_port_value * 0.0

    #First Pass
    #print df_orders.ix[ord_date,3] - Symbol
    #print df_orders.ix[ord_date,4] - Buy/Sell
    #print df_orders.ix[ord_date,5] - Qty
    for ord_date in range(0,len(order_dates)):
        close_price = df_close[df_orders.ix[ord_date,3]].ix[df_orders.index[ord_date]]
        qty = df_orders.ix[ord_date,5]
        if df_orders.ix[ord_date,4] == 'Buy':
            df_cash.ix[df_orders.index[ord_date]] = df_cash.ix[df_orders.index[ord_date]] - (qty*close_price)
        elif df_orders.ix[ord_date,4] == 'Sell':
            df_cash.ix[df_orders.index[ord_date]] = df_cash.ix[df_orders.index[ord_date]] + (qty*close_price)

        date_of_order = df_orders.index[ord_date]

        #df_cash.ix[df_orders.index[ord_date+1]:,0] = df_cash.ix[df_orders.index[ord_date]]

        for orderdate in ldt_timestamps:
            if orderdate > date_of_order:
                df_cash.ix[orderdate] = df_cash.ix[df_orders.index[ord_date]]


    ##################
    for ord_date in range(0,len(order_dates)):
        if df_orders.ix[ord_date,4] == 'Buy':
            df_own[df_orders.ix[ord_date,3]].ix[df_orders.index[ord_date]] = df_own[df_orders.ix[ord_date,3]].ix[df_orders.index[ord_date]] + df_orders.ix[ord_date,5]
        elif df_orders.ix[ord_date,4] == 'Sell':
            df_own[df_orders.ix[ord_date,3]].ix[df_orders.index[ord_date]] = df_own[df_orders.ix[ord_date,3]].ix[df_orders.index[ord_date]] - df_orders.ix[ord_date,5]
        date_of_order = df_orders.index[ord_date]
        
        for orderdate in ldt_timestamps:
            if orderdate > date_of_order:
                df_own.ix[orderdate] = df_own.ix[df_orders.index[ord_date]]


    ####################
    # Use ownership data frame and use prices to update Value data frame

    for port_sym in sym_list:
        for val_date in ldt_timestamps:
            df_hold_value[port_sym].ix[val_date] = df_own[port_sym].ix[val_date] * df_close[port_sym].ix[val_date]

        
    # Update df_total_hold_value by adding holding values from the holding values data frame

    for val_date in ldt_timestamps:
        for port_sym in sym_list:
            df_total_hold_value[0].ix[val_date] = df_total_hold_value[0].ix[val_date] + df_hold_value[port_sym].ix[val_date]

    #Update total portfolio value

    df_port_value = df_total_hold_value + df_cash
    df_port_value_changes = analysis.portvalchanges(df_port_value)
    #df_port_value.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_port_value.csv')
    #df_port_value_changes.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_port_value_changes.csv')
    maxdrawdown = analysis.maxdrawdown(df_port_value_changes)
    #print maxdrawdown
    
    '''
    df_orders.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_orders.csv')
    df_close.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_close.csv')
    df_cash.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_cash.csv')
    df_own.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_own.csv')
    df_hold_value.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_hold_value.csv')
    df_total_hold_value.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\Homework\df_total_hold_value.csv')
    '''
    df_port_value.to_csv(output_file)