import shelve
import pandas as pd


class Watchlist(object):
    def __init__(self, list_name, l_sym):
        self.watchlist_name = list_name
        self.l_symbols = l_sym

    def get_watchlist_name(self):
        return self.watchlist_name

    def get_watchlist_sym(self):
        return self.l_symbols


class WatchlistDb(object):

    WATCHLIST_DB_PATH = ''
    WATCHLIST_CSV_PATH = '../symbols/'
    WATCHLIST_FILE = 'watchlistdb.csv'
    WATCHLIST_DB = 'watchlistdb.db'

    def get_watchlists_csv(self, file_name=WATCHLIST_CSV_PATH+WATCHLIST_FILE):
        wlist = pd.read_csv(file_name)
        wlist = wlist.fillna('')

        watchlist_dict = {}

        for wl_name in wlist.columns:
            l_wlist = list(wlist[wl_name])
            try:
                while 1:
                    l_wlist.remove('')
            except:
                pass
            watchlist_dict[wl_name] = l_wlist

        return watchlist_dict

    def open_watchlistdb(self, db_name = WATCHLIST_DB_PATH+WATCHLIST_DB):
        db_con = shelve.open(db_name)
        return db_con

    def insert_one_watchlist(self, connection, wlist):
        connection[wlist.get_watchlist_name()] = wlist
        connection.close()

    def insert_multiple_watchlists(self, connection, l_wlist_dict):
        for w_list in l_wlist_dict:
            connection[w_list] = l_wlist_dict[w_list]
        connection.close()

    def get_watchlists(self, db_name):
        connection = self.open_watchlistdb(db_name=db_name)
        for w_list in connection:
            return w_list, connection[w_list]
            #print 'Watchlist Name:' + w_list
            #print 'Watchlist Symbols:' + str(connection[w_list])

    def get_watchlist_by_name(self, db_name, watchlist_name):
        conn = self.open_watchlistdb(db_name=db_name)
        return conn[watchlist_name]

if __name__ == '__main__':

    import symbols


    sym = symbols.SymbolDb()
    wlist_dict = sym.get_watchlists()
    wl = WatchlistDb()
    conn = wl.open_watchlistdb()
    wl.insert_multiple_watchlists(conn, wlist_dict)
    wl.get_watchlists('watchlistdb.db')
    ibd50 = wl.get_watchlist_by_name('watchlistdb.db', 'IBD50')
    print ibd50
