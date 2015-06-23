import pandas as pd
import numpy as np
import copy
import talib

class Indicators(object):
    """
    Input: Price DataFrame, Moving average/lookback period and standard deviation multiplier

    This function returns a dataframe with 5 columns
    Output: Prices, Moving Average, Upper BB, Lower BB and BB Val
    """

    def bb(self, l_sym, df_price, time_period, st_dev_u, st_dev_l):
        df_bb_u = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_bb_m = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_bb_l = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_bb_u[sym], df_bb_m[sym], df_bb_l[sym] = talib.BBANDS(np.asarray(df_price[sym].ix[:, 'Close']), time_period, st_dev_u, st_dev_l)
        return df_bb_u, df_bb_m, df_bb_l

    def ema(self, l_sym, df_price, time_period):
        df_ema = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_ema[sym] = talib.EMA(np.asarray(df_price[sym].ix[:, 'Close']), timeperiod=time_period)

        return df_ema

    def ma(self, l_sym, df_price, time_period):
        df_ma = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_ma[sym] = talib.MA(np.asarray(df_price[sym].ix[:, 'Close']), timeperiod=time_period)

        return df_ma

    def sma(self, l_sym, df_price, time_period):
        df_sma = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_sma[sym] = talib.SMA(np.asarray(df_price[sym].ix[:, 'Close']), timeperiod=time_period)

        return df_sma

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




