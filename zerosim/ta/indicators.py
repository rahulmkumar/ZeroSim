import pandas as pd
import numpy as np
import copy

class Indicators(object):
    """
    Input: Price DataFrame, Moving average/lookback period and standard deviation multiplier

    This function returns a dataframe with 5 columns
    Output: Prices, Moving Average, Upper BB, Lower BB and BB Val
    """
    def bb_original(self, df_price, N, K):

        columns = ['prices','ma','bb_upper','bb_lower','bb_val']

        df_bb = pd.DataFrame(index=df_price.index,columns = columns)
        df_bb = df_bb.fillna(0.00)

        df_bb['prices'] = df_price

        df_bb['ma'] = pd.rolling_mean(df_bb['prices'],window=N)
        df_bb['bb_upper'] = pd.rolling_std(df_bb['prices'],window=N)
        df_bb['bb_lower'] = pd.rolling_std(df_bb['prices'],window=N)


        df_bb['bb_upper'] = df_bb['ma'] + (K*df_bb['bb_upper'])
        df_bb['bb_lower'] = df_bb['ma'] - (K*df_bb['bb_lower'])

        df_bb['bb_val'] = (df_bb['prices'] - df_bb['ma'])/(df_bb['bb_upper']-df_bb['ma'])
        #df_bb[] = df_bb[''].fillna(0.00)

        return df_bb
    '''
    This BB function returns the upper, mid and lower as separate data frames
    '''
    def bb(self, df_price, N, K):

        df_bb_ma = copy.deepcopy(df_price)
        df_bb_ma = df_bb_ma * np.NAN

        df_bb_u = copy.deepcopy(df_bb_ma)
        #df_bb_u = df_bb_u * np.NAN

        df_bb_l = copy.deepcopy(df_bb_ma)
        #df_bb_l = df_bb_l * np.NAN


        sym_list = df_price.columns
        for sym in sym_list:
            df_bb_ma[sym] = pd.rolling_mean(df_price[sym],window=N)
            df_bb_u[sym] = pd.rolling_std(df_price[sym],window=N)
            df_bb_l[sym] = pd.rolling_std(df_price[sym],window=N)
            df_bb_u[sym] = df_bb_ma[sym] + (K*df_bb_u[sym])
            df_bb_l[sym] = df_bb_ma[sym] - (K*df_bb_l[sym])

        #df_bb['bb_val'] = (df_bb['prices'] - df_bb['ma'])/(df_bb['bb_upper']-df_bb['ma'])
        #df_bb[] = df_bb[''].fillna(0.00)

        return df_bb_ma, df_bb_u, df_bb_l

    def sma(self, df_price, N):

        df_sma = copy.deepcopy(df_price)
        df_sma = df_sma * np.NAN
        sym_list = df_price.columns
        for sym in sym_list:
            df_sma[sym] = pd.rolling_mean(df_price[sym],window=N)

        #df_price.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_prices.csv')
        #df_sma.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_sma.csv')

        return df_sma

    def channel(self, df_price, N):
        df_upper_channel = copy.deepcopy(df_price)
        df_upper_channel = df_upper_channel * np.NAN
        #df_upper_channel = df_upper_channel.fillna(0)

        df_lower_channel = copy.deepcopy(df_price)
        df_lower_channel = df_lower_channel * np.NAN
        #df_lower_channel = df_lower_channel.fillna(0)

        sym_list = df_price.columns
        for sym in sym_list:
            df_upper_channel[sym] = pd.rolling_max(df_price[sym],min_periods=1,window=N)
            df_lower_channel[sym] = pd.rolling_min(df_price[sym],min_periods=1,window=N)

        #df_price.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_prices.csv')
        #df_upper_channel.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_upper_channel.csv')
        #df_lower_channel.to_csv('C:\Users\owner\Documents\Software\Python\Quant\Examples\ZeroSum Strategy Suite\df_lower_channel.csv')

        return df_upper_channel,df_lower_channel

    def tr(self, df_close, df_high, df_low):
        sym_list = df_close.columns
        timestamps = df_close.index

        columns = ['high','low','close','hl','hcp','lcp','tr']

        df_tr = pd.DataFrame(index=df_close.index,columns = columns)
        df_tr = df_tr.fillna(0.00)

        df_symtr = copy.deepcopy(df_close)
        df_symtr = df_symtr.fillna(0.00)

        for sym in sym_list:

            df_tr['close'] = df_close[sym]
            df_tr['high'] = df_high[sym]
            df_tr['low'] = df_low[sym]

            for t_stamp in range(0,len(timestamps)):
                df_tr['hl'].ix[t_stamp] = df_tr['high'].ix[t_stamp] - df_tr['low'].ix[t_stamp]
                try:
                    df_tr['hcp'].ix[t_stamp] = abs(df_tr['high'].ix[t_stamp] - df_tr['close'].ix[t_stamp-1])
                    df_tr['lcp'].ix[t_stamp] = abs(df_tr['low'].ix[t_stamp] - df_tr['close'].ix[t_stamp-1])
                except:
                    pass
                df_tr['tr'].ix[t_stamp] = max(max(df_tr['hl'].ix[t_stamp],df_tr['hcp'].ix[t_stamp]),df_tr['lcp'].ix[t_stamp])
            df_symtr[sym] = df_tr['tr']

        return df_symtr

    # m*ATR(n)
    def atr(self, df_close, df_high, df_low, n, m):
        #df_close = d_data['close']
        sym_list = df_close.columns
        timestamps = df_close.index

        columns = ['high','low','close','tr','atr']

        df_atr = pd.DataFrame(index=df_close.index,columns = columns)
        df_atr = df_atr.fillna(0.00)

        df_symatr = copy.deepcopy(df_close)
        df_symatr = df_symatr.fillna(0.00)

        df_tr = copy.deepcopy(df_close)
        df_tr = df_tr.fillna(0.00)

        df_tr = self.tr(df_close,df_high,df_low)

        for sym in sym_list:

            df_atr['close'] = df_close[sym]
            df_atr['high'] = df_high[sym]
            df_atr['low'] = df_low[sym]
            df_atr['tr'] = df_tr[sym]

            for t_stamp in range(0,len(timestamps)):
                if t_stamp == n-1:
                    df_atr['atr'].ix[t_stamp] = df_atr['tr'].ix[t_stamp]
                elif t_stamp >= n:
                    df_atr['atr'].ix[t_stamp] = (((df_atr['atr'].ix[t_stamp-1] * (n-1))+ df_atr['tr'].ix[t_stamp]) / n) * m
                else:
                    df_atr['atr'].ix[t_stamp] = 0.00

            df_symatr[sym] = df_atr['atr']
            df_atr.to_csv('./debug/df_atrnew.csv')

        df_symatr.to_csv('./debug/df_symatrnew.csv')
        return df_symatr


    def atrold(self, df_close, df_high, df_low, n):

        #df_close = d_data['close']
        sym_list = df_close.columns
        timestamps = df_close.index

        columns = ['high','low','close','hl','hcp','lcp','tr','atr']

        df_atr = pd.DataFrame(index=df_close.index,columns = columns)
        df_atr = df_atr.fillna(0.00)

        df_symatr = copy.deepcopy(df_close)
        df_symatr = df_symatr.fillna(0)


        for sym in sym_list:

            df_atr['close'] = df_close[sym]
            df_atr['high'] = df_high[sym]
            df_atr['low'] = df_low[sym]

            for t_stamp in range(0,len(timestamps)):
                df_atr['hl'].ix[t_stamp] = df_atr['high'].ix[t_stamp] - df_atr['low'].ix[t_stamp]
                try:
                    df_atr['hcp'].ix[t_stamp] = abs(df_atr['high'].ix[t_stamp] - df_atr['close'].ix[t_stamp-1])
                    df_atr['lcp'].ix[t_stamp] = abs(df_atr['low'].ix[t_stamp] - df_atr['close'].ix[t_stamp-1])
                except:
                    pass
                df_atr['tr'].ix[t_stamp] = max(max(df_atr['hl'].ix[t_stamp],df_atr['hcp'].ix[t_stamp]),df_atr['lcp'].ix[t_stamp])
                if t_stamp == n-1:
                    df_atr['atr'].ix[t_stamp] = df_atr['tr'].ix[t_stamp]
                elif t_stamp >= n:
                    df_atr['atr'].ix[t_stamp] = ((df_atr['atr'].ix[t_stamp-1] * (n-1))+ df_atr['tr'].ix[t_stamp]) / n
                else:
                    df_atr['atr'].ix[t_stamp] = 0.00

            df_symatr[sym] = df_atr['atr']
            df_atr.to_csv('./debug/df_atr.csv')

        df_symatr.to_csv('./debug/df_symatr.csv')
        return df_symatr




    def stochastic(self, df_close, df_high, df_low, n):

        sym_list = df_close.columns
        timestamps = df_close.index
        columns = ['high','low','close','hh','ll','stoch']

        df_symstoch = pd.DataFrame(index=df_close.index,columns = columns)
        df_symstoch = df_symstoch.fillna(0.00)

        df_stoch = copy.deepcopy(df_close)
        df_stoch = df_stoch.fillna(0.0)

        for sym in sym_list:
            df_symstoch['close'] = df_close[sym]
            df_symstoch['high'] = df_high[sym]
            df_symstoch['low'] = df_low[sym]
            df_symstoch['hh'] = pd.rolling_max(df_high[sym],min_periods=1,window=n)
            df_symstoch['ll'] = pd.rolling_min(df_low[sym],min_periods=1,window=n)
            for t_stamp in range(0,len(timestamps)):
                if t_stamp >= n-1:
                    df_symstoch['stoch'].ix[t_stamp] = ((df_symstoch['close'].ix[t_stamp] - df_symstoch['ll'].ix[t_stamp])/(df_symstoch['hh'].ix[t_stamp] - df_symstoch['ll'].ix[t_stamp]))*100
            df_stoch[sym] = df_symstoch['stoch']

        df_stoch.to_csv('./debug/df_stoch.csv')
        return df_stoch

    #ema for Data Series
    def emas(self, df_series, n, m):
        timestamps = df_series.index
        const = float(2.00/(n+1))

        columns = ['price','sma','const','ema']

        df_ema = pd.DataFrame(index = df_series.index, columns = columns)
        df_ema = df_ema.fillna(0.00)

        df_ema['close'] = df_series
        df_ema['sma'] = pd.rolling_mean(df_series,window=n)

        for t_stamp in range(0,len(timestamps)):
            if t_stamp == n-1:
                df_ema['const'].ix[t_stamp] = const
                df_ema['ema'].ix[t_stamp] = (df_ema['sma'].ix[t_stamp])*m
            elif t_stamp >=n:
                df_ema['const'].ix[t_stamp] = const
                df_ema['ema'].ix[t_stamp] = ((df_ema['price'].ix[t_stamp]*const) + ((df_ema['ema'].ix[t_stamp-1])*(1-const))) * m

        return df_ema



    # ema for symbol dataframe
    def ema(self, df_close, n):

        sym_list = df_close.columns
        timestamps = df_close.index

        const = float(2.00/(n+1))

        columns = ['close','sma','const','ema']

        df_symema = pd.DataFrame(index = df_close.index, columns = columns)
        df_symema = df_symema.fillna(0.00)

        df_ema = copy.deepcopy(df_close)
        df_ema = df_ema.fillna(0.00)


        for sym in sym_list:
            df_symema['close'] = df_close[sym]
            df_symema['sma'] = pd.rolling_mean(df_close[sym],window=n)

            for t_stamp in range(0,len(timestamps)):
                if t_stamp == n-1:
                    df_symema['const'].ix[t_stamp] = const
                    df_symema['ema'].ix[t_stamp] = df_symema['sma'].ix[t_stamp]
                elif t_stamp >=n:
                    df_symema['const'].ix[t_stamp] = const
                    df_symema['ema'].ix[t_stamp] = (df_symema['close'].ix[t_stamp]*const) + ((df_symema['ema'].ix[t_stamp-1])*(1-const))
            df_ema[sym] = df_symema['ema']
            df_symema.to_csv('./debug/df_symema.csv')
        df_ema.to_csv('./debug/df_ema.csv')
        return df_ema


    def macd(self, df_close, fast, slow, sig):

        sym_list = df_close.columns
        timestamps = df_close.index

        df_emafast = self.ema(df_close,fast)
        df_emaslow = self.ema(df_close,slow)

        df_macd = copy.deepcopy(df_close)
        df_macd = df_macd.fillna(0.00)

        for sym in sym_list:
            for t_stamp in range(0,len(timestamps)):
                if t_stamp >= slow-1:
                    df_macd[sym].ix[t_stamp] = df_emafast[sym].ix[t_stamp] - df_emaslow[sym].ix[t_stamp]
                else:
                    df_macd[sym].ix[t_stamp] = 0.00

        df_signal = self.ema(df_macd,sig)

        df_macd.to_csv('./debug/df_macd.csv')
        df_signal.to_csv('./debug/df_signal.csv')

        return df_macd, df_signal

    # Moving average envelope
    #n = period
    #m = % above and below
    def mae(self, df_close, n, m):

        timestamps = df_close.index

        df_mae = copy.deepcopy(df_close)
        df_mae = df_mae * np.NAN

        df_mau = copy.deepcopy(df_close)
        df_mau = df_mau * np.NAN

        df_mal = copy.deepcopy(df_close)
        df_mal = df_mal * np.NAN

        sym_list = df_close.columns
        for sym in sym_list:
            df_mae[sym] = pd.rolling_mean(df_close[sym],window=n)


        for sym in sym_list:
            for t_stamp in range(0,len(timestamps)):
                df_mau[sym].ix[t_stamp] = df_mae[sym].ix[t_stamp] + (df_mae[sym].ix[t_stamp]*m)
                df_mal[sym].ix[t_stamp] = df_mae[sym].ix[t_stamp] - (df_mae[sym].ix[t_stamp]*m)

        return df_mae, df_mau, df_mal


    #e = ema length
    #a = atr length
    #m = atr multiplier

    def keltner(self, df_close, df_high, df_low, e, a, m):
        sym_list = df_close.columns
        timestamps = df_close.index

        df_kch_m = self.ema(df_close,e)
        df_atr = self.atr(df_close,df_high,df_low,a,m)

        df_kch_u = copy.deepcopy(df_close)
        df_kch_u = df_kch_u * np.NAN

        df_kch_l = copy.deepcopy(df_close)
        df_kch_l = df_kch_l * np.NAN

        for sym in sym_list:
            for t_stamp in range(0,len(timestamps)):
                df_kch_u[sym].ix[t_stamp] = df_kch_m[sym].ix[t_stamp] + df_atr[sym].ix[t_stamp]
                df_kch_l[sym].ix[t_stamp] = df_kch_m[sym].ix[t_stamp] - df_atr[sym].ix[t_stamp]

        return df_kch_m, df_kch_u, df_kch_l

    def adx(self, df_close, df_high, df_low, n):
        sym_list = df_close.columns
        timestamps = df_close.index

        columns = ['high','low','close','tr','pdm1','ndm1','trn','pdmn','ndmn','pdin','ndin','dindiff','dipsum','dx','adx']

        df_symadx = pd.DataFrame(index = df_close.index, columns = columns)
        df_symadx = df_symadx.fillna(0.00)

        df_adx = copy.deepcopy(df_close)
        df_adx = df_adx.fillna(0.00)

        df_tr = self.tr(df_close,df_high,df_low)

        for sym in sym_list:
            df_symadx['high'] = df_high[sym]
            df_symadx['low'] = df_low[sym]
            df_symadx['close'] = df_close[sym]
            df_symadx['tr'] = df_tr[sym]
            trn = 0.00
            pdmn = 0.00
            ndmn = 0.00
            dx = 0.00
            for t_stamp in range(0,len(timestamps)):
                try:
                    if(df_symadx['high'].ix[t_stamp]-df_symadx['high'].ix[t_stamp-1]>df_symadx['low'].ix[t_stamp-1]-df_symadx['low'].ix[t_stamp]):
                        df_symadx['pdm1'].ix[t_stamp] = max(df_symadx['high'].ix[t_stamp]-df_symadx['high'].ix[t_stamp-1],0)
                    if(df_symadx['low'].ix[t_stamp-1]-df_symadx['low'].ix[t_stamp]>df_symadx['high'].ix[t_stamp]-df_symadx['high'].ix[t_stamp-1]):
                        df_symadx['ndm1'].ix[t_stamp] = max(df_symadx['low'].ix[t_stamp-1]-df_symadx['low'].ix[t_stamp],0)
                    if t_stamp < n+1:
                        trn = trn + df_symadx['trn'].ix[t_stamp]
                        pdmn = pdmn + df_symadx['pdmn'].ix[t_stamp]
                        ndmn = ndmn + df_symadx['ndmn'].ix[t_stamp]
                    if t_stamp == n+1:
                        df_symadx['trn'].ix[t_stamp] = trn
                        df_symadx['pdmn'].ix[t_stamp] = pdmn
                        df_symadx['ndmn'].ix[t_stamp] = ndmn
                    elif t_stamp > n+1:
                        df_symadx['trn'].ix[t_stamp] = df_symadx['trn'].ix[t_stamp-1] - (df_symadx['trn'].ix[t_stamp-1]/n) + df_symadx['tr'].ix[t_stamp]
                        df_symadx['pdmn'].ix[t_stamp] = df_symadx['pdmn'].ix[t_stamp-1] - (df_symadx['pdmn'].ix[t_stamp-1]/n) + df_symadx['pdm1'].ix[t_stamp]
                        df_symadx['ndmn'].ix[t_stamp] = df_symadx['ndmn'].ix[t_stamp-1] - (df_symadx['ndmn'].ix[t_stamp-1]/n) + df_symadx['ndm1'].ix[t_stamp]
                    if t_stamp >= n+1:
                        df_symadx['pdin'].ix[t_stamp] = (df_symadx['pdmn'].ix[t_stamp] / df_symadx['trn'].ix[t_stamp])*100
                        df_symadx['ndin'].ix[t_stamp] = (df_symadx['ndmn'].ix[t_stamp] / df_symadx['trn'].ix[t_stamp])*100
                        df_symadx['dindiff'].ix[t_stamp] = abs(df_symadx['pdin'].ix[t_stamp] - df_symadx['ndin'].ix[t_stamp])
                        df_symadx['dipsum'].ix[t_stamp] = abs(df_symadx['pdin'].ix[t_stamp] + df_symadx['ndin'].ix[t_stamp])
                        df_symadx['dx'].ix[t_stamp] = (df_symadx['dindiff'].ix[t_stamp] / df_symadx['dipsum'].ix[t_stamp])*100
                    if t_stamp < 2*n:
                        dx = dx + df_symadx['dx'].ix[t_stamp]
                    if t_stamp == 2*n:
                        df_symadx['adx'].ix[t_stamp] = dx/(2*n)
                    if t_stamp > 2*n:
                        df_symadx['adx'].ix[t_stamp] = ((df_symadx['adx'].ix[t_stamp-1] * (n-1)) + df_symadx['dx'].ix[t_stamp])/n
                except:
                    pass
            df_adx[sym] = df_symadx['adx']

        df_symadx.to_csv('./debug/df_symadx.csv')
        df_adx.to_csv('./debug/df_adx.csv')

        return df_adx


    def adxold(self, df_close, df_high, df_low, n):
        sym_list = df_close.columns
        timestamps = df_close.index

        columns = ['high','low','close','atr','pdm1','ndm1','emap','eman','pdin','ndin','dx','adx']

        df_symadx = pd.DataFrame(index = df_close.index, columns = columns)
        df_symadx = df_symadx.fillna(0.00)

        df_adx = copy.deepcopy(df_close)
        df_adx = df_adx.fillna(0.00)

        df_atr = self.atr(df_close,df_high,df_low,n,1)

        for sym in sym_list:
            df_symadx['high'] = df_high[sym]
            df_symadx['low'] = df_low[sym]
            df_symadx['close'] = df_close[sym]
            df_symadx['atr'] = df_atr[sym]
            for t_stamp in range(0,len(timestamps)):
                try:
                    upmove = df_symadx['high'].ix[t_stamp] - df_symadx['high'].ix[t_stamp-1]
                    downmove =  df_symadx['low'].ix[t_stamp-1] - df_symadx['low'].ix[t_stamp]
                except:
                    pass

                if upmove > downmove and upmove > 0.00:
                    df_symadx['pdm1'].ix[t_stamp] = upmove
                else:
                    df_symadx['pdm1'].ix[t_stamp] = 0.00

                if downmove > upmove and downmove > 0.00:
                    df_symadx['ndm1'].ix[t_stamp] = downmove
                else:
                    df_symadx['ndm1'].ix[t_stamp] = 0.00

            df_ema_pdm1 = self.emas(df_symadx['pdm1'],n,1)
            df_ema_ndm1 = self.emas(df_symadx['ndm1'],n,1)

            df_symadx['emap'] = df_ema_pdm1['ema']
            df_symadx['eman'] = df_ema_ndm1['ema']

            for t_stamp in range(0,len(timestamps)):
                if t_stamp >= n-1:
                    df_symadx['pdin'].ix[t_stamp] = (df_symadx['emap'].ix[t_stamp]/df_symadx['atr'].ix[t_stamp])*100
                    df_symadx['ndin'].ix[t_stamp] = (df_symadx['eman'].ix[t_stamp]/df_symadx['atr'].ix[t_stamp])*100
                    df_symadx['dx'].ix[t_stamp] = abs((df_symadx['pdin'].ix[t_stamp]-df_symadx['ndin'].ix[t_stamp])/(df_symadx['pdin'].ix[t_stamp]+df_symadx['ndin'].ix[t_stamp]))

            df_adx_val = self.emas(df_symadx['dx'],n,100)
            #df_symadx['adx'] = emas(df_symadx['dx'],n,100)
            df_symadx['adx'] = df_adx_val['ema']
            df_adx[sym] = df_adx_val['ema']
        df_symadx.to_csv('./debug/df_symadx.csv')
        #df_adx.to_csv('./debug/df_adx.csv')
        return df_adx


    def ichimoku(self, df_close, df_high, df_low):
        '''
        df_ichimoku_tenkan_u, df_ichimoku_tenkan_l = indicators.channel(df_close,9)
        df_ichimoku_kijun_u, df_ichimoku_kijun_l = indicators.channel(df_close,26)
        df_senkou_a
        df_senkou_b_u, df_senkou_b_l = indicators.channel(df_close,52)

        sym_list = df_close.columns
        '''
        timestamps = df_close.index
        sym_list = df_close.columns

        df_ichimoku_tenkan_u = copy.deepcopy(df_close)
        df_ichimoku_tenkan_u = df_ichimoku_tenkan_u * np.NAN

        df_ichimoku_tenkan_l = copy.deepcopy(df_close)
        df_ichimoku_tenkan_l = df_ichimoku_tenkan_l * np.NAN

        df_ichimoku_kijun_u = copy.deepcopy(df_close)
        df_ichimoku_kijun_u = df_ichimoku_kijun_u * np.NAN

        df_ichimoku_kijun_l = copy.deepcopy(df_close)
        df_ichimoku_kijun_l = df_ichimoku_kijun_l * np.NAN

        df_ichimoku_kijun = copy.deepcopy(df_close)
        df_ichimoku_kijun = df_ichimoku_kijun * np.NAN

        df_ichimoku_tenkan = copy.deepcopy(df_close)
        df_ichimoku_tenkan = df_ichimoku_tenkan * np.NAN

        for sym in sym_list:
            df_ichimoku_tenkan_u[sym] = pd.rolling_max(df_high[sym],min_periods=1,window=9)
            df_ichimoku_tenkan_l[sym] = pd.rolling_min(df_low[sym],min_periods=1,window=9)

            df_ichimoku_kijun_u[sym] = pd.rolling_max(df_high[sym],min_periods=1,window=26)
            df_ichimoku_kijun_l[sym] = pd.rolling_min(df_low[sym],min_periods=1,window=26)

            for t_stamp in timestamps:
                df_ichimoku_tenkan[sym].ix[t_stamp] = (df_ichimoku_tenkan_u[sym].ix[t_stamp] + df_ichimoku_tenkan_l[sym].ix[t_stamp])/2
                df_ichimoku_kijun[sym].ix[t_stamp] = (df_ichimoku_kijun_u[sym].ix[t_stamp] + df_ichimoku_kijun_l[sym].ix[t_stamp])/2


        return df_ichimoku_tenkan, df_ichimoku_kijun

    '''
    This function creates the TTM squeeze indicators:
    Bollinger Bands: 20,2
    Keltner Channels: 20,1.5
    '''
    def TTM_squeeze(self, df_price, df_high, df_low, N, K, e, a, m):
        sym_list = df_price.columns
        timestamps = df_price.index

        df_bb_ma = copy.deepcopy(df_price)
        df_bb_ma = df_bb_ma * np.NAN

        df_bb_u = copy.deepcopy(df_bb_ma)
        #df_bb_u = df_bb_u * np.NAN

        df_bb_l = copy.deepcopy(df_bb_ma)
        #df_bb_l = df_bb_l * np.NAN

        df_kch_u = copy.deepcopy(df_price)
        df_kch_u = df_kch_u * np.NAN

        df_kch_l = copy.deepcopy(df_price)
        df_kch_l = df_kch_l * np.NAN

        df_kch_m = self.ema(df_price,e)
        df_atr = self.atr(df_price,df_high,df_low,a,m)

        for sym in sym_list:
            df_bb_ma[sym] = pd.rolling_mean(df_price[sym],window=N)
            df_bb_u[sym] = pd.rolling_std(df_price[sym],window=N)
            df_bb_l[sym] = pd.rolling_std(df_price[sym],window=N)
            df_bb_u[sym] = df_bb_ma[sym] + (K*df_bb_u[sym])
            df_bb_l[sym] = df_bb_ma[sym] - (K*df_bb_l[sym])
            for t_stamp in range(0,len(timestamps)):
                df_kch_u[sym].ix[t_stamp] = df_kch_m[sym].ix[t_stamp] + df_atr[sym].ix[t_stamp]
                df_kch_l[sym].ix[t_stamp] = df_kch_m[sym].ix[t_stamp] - df_atr[sym].ix[t_stamp]


        return df_bb_ma, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l
