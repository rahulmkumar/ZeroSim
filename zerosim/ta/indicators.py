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

        df_bb_u = pd.DataFrame(columns=l_sym, index=df_price.index)
        df_bb_m = pd.DataFrame(columns=l_sym, index=df_price.index)
        df_bb_l = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_bb_u[sym], df_bb_m[sym], df_bb_l[sym] = talib.BBANDS(np.asarray(df_price[sym]), timeperiod=time_period, nbdevup=st_dev_u, nbdevdn=st_dev_l)
            except:
                pass
        return df_bb_u, df_bb_m, df_bb_l

    def ema(self, l_sym, df_price, time_period):

        df_ema = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_ema[sym] = talib.EMA(np.asarray(df_price[sym]), timeperiod=time_period)
            except:
                pass

        return df_ema

    def ma(self, l_sym, df_price, time_period):

        df_ma = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_ma[sym] = talib.MA(np.asarray(df_price[sym]), timeperiod=time_period)
            except:
                pass

        return df_ma

    def sma(self, l_sym, df_price, time_period):

        df_sma = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_sma[sym] = talib.SMA(np.asarray(df_price[sym]), timeperiod=time_period)
            except:
                pass

        return df_sma

    def adx(self, l_sym, df_high, df_low, df_close, time_period):

        df_adx = pd.DataFrame(columns=l_sym, index=df_high.index)

        for sym in l_sym:
            try:
                df_adx[sym] = talib.ADX(high=np.asarray(df_high[sym]), low=np.asarray(df_low[sym]), close=np.asarray(df_close[sym]), timeperiod = time_period)
            except:
                pass

        return df_adx

    def mom(self, l_sym, df_price, time_period):

        df_mom = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_mom[sym] = talib.MOM(np.asarray(df_price[sym]), timeperiod = time_period)
            except:
                pass

        return df_mom

    def atr(self, l_sym, df_high, df_low, df_close, time_period):

        df_atr = pd.DataFrame(columns=l_sym, index=df_high.index)

        for sym in l_sym:
            try:
                df_atr[sym] = talib.ATR(high=np.asarray(df_high[sym]), low=np.asarray(df_low[sym]), close=np.asarray(df_close[sym]), timeperiod=time_period)
            except:
                pass
        return df_atr

    def macd(self, l_sym, df_price, fast_period, slow_period, signal_period):

        df_macd = pd.DataFrame(columns=l_sym, index=df_price.index)
        df_macdsignal = pd.DataFrame(columns=l_sym, index=df_price.index)
        df_macdhist = pd.DataFrame(columns=l_sym, index=df_price.index)

        for sym in l_sym:
            try:
                df_macd[sym], df_macdsignal[sym], df_macdhist[sym] = talib.MACD(np.asarray(df_price[sym]), fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period)
            except:
                pass

        return df_macd, df_macdsignal, df_macdhist

    def wavec(self, l_sym, df_three, df_four, df_five):

        df_ca = pd.DataFrame(columns=l_sym, index=df_three.index)
        df_cb = pd.DataFrame(columns=l_sym, index=df_three.index)

        for sym in l_sym:
            df_ca[sym] = df_four[sym] - df_five[sym]
            df_cb[sym] = df_three[sym] - df_four[sym]

        return df_ca, df_cb

    def waveb(self, l_sym, df_two, df_three, df_four):

        df_ba = pd.DataFrame(columns=l_sym, index=df_two.index)
        df_bb = pd.DataFrame(columns=l_sym, index=df_two.index)

        for sym in l_sym:
            df_ba[sym] = df_three[sym] - df_four[sym]
            df_bb[sym] = df_two[sym] - df_three[sym]

        return df_ba, df_bb

    def wavea(self, l_sym, df_one, df_two, df_three):

        df_aa = pd.DataFrame(columns=l_sym, index=df_one.index)
        df_ab = pd.DataFrame(columns=l_sym, index=df_one.index)

        for sym in l_sym:
            df_aa[sym] = df_two[sym] - df_three[sym]
            df_ab[sym] = df_one[sym] - df_two[sym]

        return df_aa, df_ab

    def keltner(self, l_sym, df_high, df_low, df_close, ema_period, atr_period, multiplier):

        df_kch_u = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_kch_l = pd.DataFrame(columns=l_sym, index=df_high.index)

        df_kch_m = self.ema(l_sym, df_close, time_period=ema_period)
        df_atr = self.atr(l_sym, df_high, df_low, df_close, time_period=atr_period)

        for sym in l_sym:
            df_kch_u[sym] = df_kch_m[sym] + (multiplier * df_atr[sym])
            df_kch_l[sym] = df_kch_m[sym] - (multiplier * df_atr[sym])

        return df_kch_u, df_kch_m, df_kch_l

    def ichimoku(self, l_sym, df_high, df_low):

        df_ichimoku_tenkan_u = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_ichimoku_tenkan_l = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_ichimoku_kijun_u = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_ichimoku_kijun_l = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_ichimoku_kijun = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_ichimoku_tenkan = pd.DataFrame(columns=l_sym, index=df_high.index)

        for sym in l_sym:
            df_ichimoku_tenkan_u[sym] = pd.rolling_max(df_high[sym], min_periods=1, window=9)
            df_ichimoku_tenkan_l[sym] = pd.rolling_min(df_low[sym], min_periods=1, window=9)

            df_ichimoku_kijun_u[sym] = pd.rolling_max(df_high[sym], min_periods=1, window=26)
            df_ichimoku_kijun_l[sym] = pd.rolling_min(df_low[sym], min_periods=1, window=26)

            df_ichimoku_tenkan[sym] = (df_ichimoku_tenkan_u[sym] + df_ichimoku_tenkan_l[sym])/2
            df_ichimoku_kijun[sym] = (df_ichimoku_kijun_u[sym] + df_ichimoku_kijun_l[sym])/2

        return df_ichimoku_tenkan, df_ichimoku_kijun

    '''
    This function creates the TTM squeeze indicators:
    Bollinger Bands: 20,2
    Keltner Channels: 20,1.5
    '''
    def ttm_squeeze(self, l_sym, df_high, df_low, df_close, bb_ma, stdev_multiplier, ema_period, atr_period, atr_multiplier):

        df_kch_u = pd.DataFrame(columns=l_sym, index=df_high.index)
        df_kch_l = pd.DataFrame(columns=l_sym, index=df_high.index)

        df_kch_m = self.ema(l_sym, df_close, ema_period)
        df_atr = self.atr(l_sym, df_high, df_low, df_close, atr_period)
        df_bb_u, df_bb_m, df_bb_l = self.bb(l_sym, df_close, bb_ma, stdev_multiplier, stdev_multiplier)

        for sym in l_sym:
            df_kch_u[sym] = df_kch_m[sym] + (atr_multiplier * df_atr[sym])
            df_kch_l[sym] = df_kch_m[sym] - (atr_multiplier * df_atr[sym])

        return df_bb_m, df_bb_u, df_bb_l, df_kch_m, df_kch_u, df_kch_l
