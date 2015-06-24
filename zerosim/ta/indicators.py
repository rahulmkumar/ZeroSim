import pandas as pd
import numpy as np
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
            df_bb_u[sym], df_bb_m[sym], df_bb_l[sym] = talib.BBANDS(np.asarray(df_price[sym].ix[:, 'Close']),
                                                                    timeperiod=time_period, nbdevup=st_dev_u,
                                                                    nbdevdn=st_dev_l)
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

    def adx(self, l_sym, df_price, time_period):
        df_adx = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_adx[sym] = talib.ADX(high=np.asarray(df_price[sym].ix[:, 'High']), low=np.asarray(df_price[sym].ix[:, 'Low']),
                                    close=np.asarray(df_price[sym].ix[:, 'Close']), timeperiod = time_period)

        return df_adx

    def mom(self, l_sym, df_price, time_period):
        df_mom = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_mom[sym] = talib.MOM(np.asarray(df_price[sym].ix[:, 'Close']), timeperiod = time_period)

        return df_mom

    def atr(self, l_sym, df_price, time_period):
        df_atr = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_atr[sym] = talib.ATR(high=np.asarray(df_price[sym].ix[:, 'High']), low=np.asarray(df_price[sym].ix[:, 'Low']),
                                    close=np.asarray(df_price[sym].ix[:, 'Close']), timeperiod=time_period)
        return df_atr

    def keltner(self, l_sym, df_price, ema_period, atr_period, multiplier):

        df_kch_u = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_kch_l = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        df_kch_m = self.ema(l_sym, df_price, time_period=ema_period)
        df_atr = self.atr(l_sym, df_price, time_period=atr_period)

        for sym in l_sym:
            df_kch_u[sym] = df_kch_m[sym] + (multiplier * df_atr[sym])
            df_kch_l[sym] = df_kch_m[sym] - (multiplier * df_atr[sym])

        return df_kch_u, df_kch_m, df_kch_l

    def ichimoku(self, l_sym, df_price):

        df_ichimoku_tenkan_u = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_ichimoku_tenkan_l = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_ichimoku_kijun_u = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_ichimoku_kijun_l = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_ichimoku_kijun = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_ichimoku_tenkan = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        for sym in l_sym:
            df_ichimoku_tenkan_u[sym] = pd.rolling_max(df_price[sym].ix[:,'High'], min_periods=1, window=9)
            df_ichimoku_tenkan_l[sym] = pd.rolling_min(df_price[sym].ix[:,'Low'], min_periods=1, window=9)

            df_ichimoku_kijun_u[sym] = pd.rolling_max(df_price[sym].ix[:,'High'], min_periods=1, window=26)
            df_ichimoku_kijun_l[sym] = pd.rolling_min(df_price[sym].ix[:,'Low'], min_periods=1, window=26)

            df_ichimoku_tenkan[sym] = (df_ichimoku_tenkan_u[sym] + df_ichimoku_tenkan_l[sym])/2
            df_ichimoku_kijun[sym] = (df_ichimoku_kijun_u[sym] + df_ichimoku_kijun_l[sym])/2

        return df_ichimoku_tenkan, df_ichimoku_kijun

    '''
    This function creates the TTM squeeze indicators:
    Bollinger Bands: 20,2
    Keltner Channels: 20,1.5
    '''
    def ttm_squeeze(self, l_sym, df_price, bb_ma, stdev_multiplier, ema_period, atr_period, atr_multiplier):

        df_kch_u = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)
        df_kch_l = pd.DataFrame(columns=l_sym, index=df_price[l_sym[0]].index)

        df_kch_m = self.ema(l_sym, df_price, ema_period)
        df_atr = self.atr(l_sym, df_price, atr_period)
        df_bb_u, df_bb_m, df_bb_l = self.bb(l_sym, df_price, bb_ma, stdev_multiplier, stdev_multiplier)

        for sym in l_sym:
            df_kch_u[sym] = df_kch_m[sym] + (atr_multiplier * df_atr[sym])
            df_kch_l[sym] = df_kch_m[sym] - (atr_multiplier * df_atr[sym])

        return df_bb_m, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l
