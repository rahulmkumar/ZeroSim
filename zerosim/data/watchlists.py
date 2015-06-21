import shelve


class Watchlist(object):
    def __init__(self, list_name, l_sym):
        self.watchlist_name = list_name
        self.l_symbols = l_sym

    def get_watchlist_name(self):
        return self.watchlist_name

    def get_watchlist_sym(self):
        return self.l_symbols


class WatchlistDb(object):

    WATCHLIST_DB_PATH = '/'
    WATCHLIST_DB = 'watchlistdb'

    def open_watchlistdb(self, db_path = WATCHLIST_DB_PATH, db_name = WATCHLIST_DB):
        db_con = shelve.open(db_path+db_name)
        return db_con

    def insert_one_watchlist(self, connection, wlist):
        connection[wlist.get_watchlist_name()] = wlist
        connection.close()

    def insert_multiple_watchlists(self, connection, l_wlist):
        for w_list in l_wlist:
            connection[w_list.get_watchlist_name()] = w_list
        connection.close()

if __name__ == '__main__':

    import pandas as pd

    wlist = pd.read_csv('../../symbols/watchlistdb.csv')
    wlist = wlist.fillna('')

    print wlist.columns

    print list(wlist[wlist.columns[0]])

    l_wlist = list(wlist[wlist.columns[0]])

    nacount = list(wlist[wlist.columns[0]]).count('')

    try:
        while 1:
            l_wlist.remove('')
    except:
        pass

    print l_wlist

