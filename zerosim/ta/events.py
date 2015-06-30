import pandas as pd
import numpy as np
import copy
import csv
import indicators
import math


class Events(object):

    def find_bb_events(self, ls_symbols, df_data, benchmark):
        print 'Inside find_bb_events'
        ''' Finding the event dataframe '''
        #df_close = d_data['close']
        df_close = df_data
        ts_market = df_close[benchmark]
        #ts_market = df_close['SPY']

        # Find Bollinger Band Value for the market
        df_bb_market = indicators.bb(ts_market,20,1)

        print "Finding Events"

        # Creating an empty dataframe
        df_events = copy.deepcopy(df_close)
        df_events = df_events * np.NAN

        # Time stamps for the event range
        ldt_timestamps = df_close.index

        #Last Trading date of the year
        last_date = ldt_timestamps[len(ldt_timestamps)-1]
        last_trade_date = ldt_timestamps[(len(ldt_timestamps)-1)-5]

        with open('mydata.csv','wb') as f:
            writer = csv.writer(f,delimiter=',',dialect='excel')
            for s_sym in ls_symbols:
                df_bb_sym = indicators.bb(df_close[s_sym],20,1)
                for i in range(1, len(ldt_timestamps)):
                    if df_bb_sym['bb_val'].ix[ldt_timestamps[i]] <= -2 and df_bb_sym['bb_val'].ix[ldt_timestamps[i-1]] >= -2 and df_bb_market['bb_val'].ix[ldt_timestamps[i]]>= 1:
                        df_events[s_sym].ix[ldt_timestamps[i]] = 1
                        writer.writerow([ldt_timestamps[i].strftime('%Y'),ldt_timestamps[i].strftime('%m'),ldt_timestamps[i].strftime('%d'),s_sym,'Buy',100])
                        try:
                            writer.writerow([ldt_timestamps[i+5].strftime('%Y'),ldt_timestamps[i+5].strftime('%m'),ldt_timestamps[i+5].strftime('%d'),s_sym,'Sell',100])
                        except:
                            writer.writerow([ldt_timestamps[-1].strftime('%Y'),ldt_timestamps[-1].strftime('%m'),ldt_timestamps[-1].strftime('%d'),s_sym,'Sell',100])

        return df_events

    '''
    Lifetime highs within the past n periods
    '''
    def lifetimehigh(self, df_close, n):

        sym_col = df_close.columns
        sym_idx = ['lt_high']

        df_lt_high = pd.DataFrame(index = sym_idx, columns= sym_col)
        df_lt_high = df_lt_high.fillna(0)

        #print df_close.columns

        for sym in df_close.columns:
            lifetimehigh = df_close[sym].max()
            #print 'lifetimehigh:'+str(lifetimehigh)
            if df_close[sym].ix[-1] > 1:
                for time_idx in range(-1, (n+1)*(-1), -1):
                    #print 'time_idx:'+str(time_idx)
                    if df_close[sym].ix[time_idx] >= lifetimehigh:
                        df_lt_high[sym].ix[0] = 1
        return df_lt_high

    def event_prices(self, df_criteria_sym, df_event):

        symbols = df_event.columns

        #df_criteria_sym.to_csv('./debug/df_criteria_sym.csv')

        for sym in symbols:
            if df_event[sym].ix[0] == 0:
                del df_criteria_sym[sym]

        return df_criteria_sym

    def rising_scan(self, df_price, l_sym=[]):

        trig_sym = []
        if len(l_sym) == 0:
            symbols = df_price.columns
        else:
            symbols = l_sym
        timestamps = df_price.index[-2:]

        df_rising = pd.DataFrame(index=timestamps, columns=symbols)

        for sym in df_price.columns:
            try:
                if df_price[sym].ix[-1] > df_price[sym].ix[-2]:
                    df_rising[sym].ix[-1] = 1
                    trig_sym.append(sym)
            except:
                pass
        return trig_sym

    def crossabove_scan(self, df1, df2, l_sym=[]):

        if len(l_sym) == 0:
            symbols = df2.columns
        else:
            symbols = l_sym
        timestamps = df2.index[-2:]
        trig_sym = []
        df1 = df1.astype(float)
        edf_crossabv = pd.DataFrame(index = timestamps, columns = symbols)
        edf_crossabv = edf_crossabv.fillna(0)

        for sym in df1.columns:
            try:
                if df1[sym].ix[-2] < df2[sym].ix[-2] and df1[sym].ix[-1] > df2[sym].ix[-1]:
                    edf_crossabv[sym].ix[-1] = 1
                    trig_sym.append(sym)
            except:
                pass
        return trig_sym

    '''
    When df1 crosses above df2
    Current value of df1 is above previous value of df2
    '''
    def crossabove_historical(self, df1, df2):
        symbols = df2.columns
        timestamps = df2.index
        df1 = df1.astype(float)
        #print timestamps
        #print df1
        #print df2
        edf_crossabv = pd.DataFrame(index = timestamps, columns = symbols)
        edf_crossabv = edf_crossabv.fillna(0)
        #print 'in crossabove'
        #for symb in df1.columns:
            #print symb

        for sym in df1.columns:
            for ts in range(0,len(timestamps)):
                try:
                    if df1[sym].ix[ts-1] < df2[sym].ix[ts-1] and df1[sym].ix[ts] > df2[sym].ix[ts]:
                        edf_crossabv[sym].ix[ts] = 1
                except:
                    pass
        return edf_crossabv

    '''
    When df1 crosses below df2
    '''
    def crossbelow_df(self, df1, df2):

        symbols = df1.columns
        timestamps = df1.index

        edf_crossblw = pd.DataFrame(index = timestamps, columns = symbols)
        edf_crossblw = edf_crossblw.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                try:
                    if df1[sym].ix[ts-1] > df2[sym].ix[ts-1] and df1[sym].ix[ts] < df2[sym].ix[ts]:
                        edf_crossblw[sym].ix[ts] = 1
                except:
                    pass

        return edf_crossblw

    '''
    When df1 crosses above number
    '''
    def crossabove_N(self, df1, N):

        symbols = df1.columns
        timestamps = df1.index

        edf_crossabv = pd.DataFrame(index = timestamps, columns = symbols)
        edf_crossabv = edf_crossabv.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                try:
                    if df1[sym].ix[ts-1] < N and df1[sym].ix[ts] > N:
                        edf_crossabv[sym].ix[ts] = 1
                except:
                    pass

        return edf_crossabv

    '''
    When df1 crosses below number
    '''
    def crossbelow_N(self, df1, N):
        symbols = df1.columns
        timestamps = df1.index

        edf_crossblw = pd.DataFrame(index = timestamps, columns = symbols)
        edf_crossblw = edf_crossblw.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                try:
                    if df1[sym].ix[ts-1] > N and df1[sym].ix[ts] < N:
                        edf_crossblw[sym].ix[ts] = 1
                except:
                    pass

        return edf_crossblw

    '''
    When df1 is above df2
    '''
    def is_above_scan(self, df1, df2):

        #if len(l_sym) == 0:
        symbols = df1.columns
        #else:
        #    symbols = l_sym
        timestamps = df1.index[-2:]
        df1 = df1.astype(float)
        trig_sym = []

        edf_isabove = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isabove = edf_isabove.fillna(0)

        for sym in symbols:
            diff = df1[sym].ix[-1] - df2[sym].ix[-1]
            if diff > 0:
                edf_isabove[sym].ix[-1] = 1
                trig_sym.append(sym)

        return trig_sym

    '''
    When df1 is above df2
    '''
    def is_above_historical(self, df1, df2):

        symbols = df1.columns
        timestamps = df1.index

        edf_isabove = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isabove = edf_isabove.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df1[sym].ix[ts] > df2[sym].ix[ts]:
                    edf_isabove[sym].ix[ts] = 1

        return edf_isabove

    '''
    When df1 is below df2
    '''
    def is_below_scan(self, df1, df2, l_sym=[]):

        if len(l_sym) == 0:
            symbols = df1.columns
        else:
            symbols = l_sym
        timestamps = df1.index[-2:]

        trig_sym = []

        edf_isbelow = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isbelow = edf_isbelow.fillna(0)

        for sym in symbols:
            if df1[sym].ix[-1] < df2[sym].ix[-1]:
                edf_isbelow[sym].ix[-1] = 1
                trig_sym.append(sym)

        return trig_sym

    '''
    When df1 is below df2
    '''
    def is_below_historical(self, df1, df2):
        symbols = df1.columns
        timestamps = df1.index

        edf_isbelow = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isbelow = edf_isbelow.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df1[sym].ix[ts] < df2[sym].ix[ts]:
                    edf_isbelow[sym].ix[ts] = 1

        return edf_isbelow
    # When df has changed direction to up
    def turningup_scan(self, df1, l_sym=[]):

        if len(l_sym) == 0:
            symbols = df1.columns
        else:
            symbols = l_sym

        trig_sym = []

        timestamps = df1.index[-3:]
        df_turningup = pd.DataFrame(index = timestamps, columns = symbols)

        for sym in symbols:
            if df1[sym].ix[-3] > df1[sym].ix[-2] and df1[sym].ix[-1] > df1[sym].ix[-2]:
                df_turningup[sym].ix[-1] = 1
                trig_sym.append(sym)

    '''
    When df1 is between df2 and df3
    '''
    def is_between_df(self, df1, df2, df3):
        symbols = df1.columns
        timestamps = df1.index

        edf_isbetween = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isbetween = edf_isbetween.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df1[sym].ix[ts] > df2[sym].ix[ts] and df1[sym].ix[ts] < df3[sym].ix[ts]:
                    edf_isbetween[sym].ix[ts] = 1

        return edf_isbetween

    '''
    When df1 is above a number
    '''
    def is_above_N(self, df1, N):
        symbols = df1.columns
        timestamps = df1.index

        edf_isabove = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isabove = edf_isabove.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df1[sym].ix[ts] > N:
                    edf_isabove[sym].ix[ts] = 1

        return edf_isabove

    '''
    When df1 is below a number
    '''
    def is_below_N(self, df1, N):
        symbols = df1.columns
        timestamps = df1.index

        edf_isbelow = pd.DataFrame(index = timestamps, columns = symbols)
        edf_isbelow = edf_isbelow.fillna(0)

        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df1[sym].ix[ts] < N:
                    edf_isbelow[sym].ix[ts] = 1

        return edf_isbelow

    '''
     Stock above, between and below F and S SMA.
     If F > S, then number is > 0 else < 0
    '''
    def SMA_level_stats(self, df1, F, S):
        symbols = df1.columns
        timestamps = df1.index

        df_SMA_stats = pd.DataFrame(index = timestamps, columns = symbols)
        df_SMA_stats = df_SMA_stats.fillna(0)

        stats_col = ['FLS','FGS','FIFTY','TWOH','TWOF','NFIFTY','NTWOH','NTWOF','SPY']

        df_stats = pd.DataFrame(index = timestamps, columns = stats_col)
        df_stats = df_stats.fillna(0)

        df_sma_fast = indicators.sma(df1,F)
        df_sma_slow = indicators.sma(df1,S)

        for ts in range(0,len(timestamps)):
            df_stats['SPY'].ix[ts] = df1['SPY'].ix[ts]
            for sym in symbols:
                if df_sma_fast[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                    df_stats['FGS'].ix[ts] = df_stats['FGS'].ix[ts] + 1
                    if df1[sym].ix[ts] > df_sma_fast[sym].ix[ts]:
                        df_stats['FIFTY'].ix[ts] = df_stats['FIFTY'].ix[ts] + 1
                    if df1[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                        df_stats['TWOH'].ix[ts] = df_stats['TWOH'].ix[ts] + 1
                    if df1[sym].ix[ts] < df_sma_fast[sym].ix[ts] and df1[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                        df_stats['TWOF'].ix[ts] = df_stats['TWOF'].ix[ts] + 1
                elif df_sma_fast[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                    df_stats['FLS'].ix[ts] = df_stats['FLS'].ix[ts] + 1
                    if df1[sym].ix[ts] < df_sma_fast[sym].ix[ts]:
                        df_stats['NFIFTY'].ix[ts] = df_stats['NFIFTY'].ix[ts] + 1
                    if df1[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                        df_stats['NTWOH'].ix[ts] = df_stats['NTWOH'].ix[ts] + 1
                    if df1[sym].ix[ts] > df_sma_fast[sym].ix[ts] and df1[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                        df_stats['NTWOF'].ix[ts] = df_stats['NTWOF'].ix[ts] + 1
        df_stats.to_csv('./debug/df_stats.csv')
        return df_SMA_stats
    '''
        for sym in symbols:
            for ts in range(0,len(timestamps)):
                if df_sma_fast[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                    if df1[sym].ix[ts] > df_sma_fast[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = 50
                    if df1[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = 200
                    if df1[sym].ix[ts] < df_sma_fast[sym].ix[ts] and df1[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = 250
                elif df_sma_fast[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                    if df1[sym].ix[ts] < df_sma_fast[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = -50
                    if df1[sym].ix[ts] > df_sma_slow[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = -200
                    if df1[sym].ix[ts] > df_sma_fast[sym].ix[ts] and df1[sym].ix[ts] < df_sma_slow[sym].ix[ts]:
                        df_SMA_stats[sym].ix[ts] = -250

    '''



    '''
    # within p% of the highest high attained in the previous n days
    def highs(df, n, p):

    # When df is rising
    def rising(df):

    # When df is falling
    def falling(df):

    # When df has changed direction to down
    def turningdn(df):


    def swing_high(df_open, df_high, df_low, df_close):
    def swing_low(df_open, df_high, df_low, df_close):

    '''
