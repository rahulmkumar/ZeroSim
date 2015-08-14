import data
import datetime


if __name__ == '__main__':
    # Refresh Watchlist
    watch_list = data.WatchlistDb()
    updated_dict = watch_list.get_watchlists_csv('../symbols/watchlistdb.csv')

    conn = watch_list.open_watchlistdb('data/watchlistdb.db')
    watch_list.insert_multiple_watchlists(conn, updated_dict)

    # Return watchlist
    ibd50 = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBD50')
    biotech = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'Biotech')
    ETFOptions = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'ETFOptions')
    ibdlow = watch_list.get_watchlist_by_name('data/watchlistdb.db', 'IBDLOW')

    # Technology Stocks
    yahoo_stocks = data.SymbolDb()
    tech = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Volume='1000000', Sector='Technology')
    biotech1 = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Volume='1000000', Industry='Bio')
    pricegt_ten = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Mcap=[500,5000], Pricegt=10)
    pricelt_ten = yahoo_stocks.get_symbols(source='Yahoo', Country='USA', Mcap=[500,5000], Pricelt=10)

    options = list(set(ibd50 + biotech + ETFOptions + tech + biotech1))

    blueprint = list(set(pricegt_ten + pricelt_ten + ibdlow))

    download_sym = list(set(options + blueprint))

    # Get data for watchlist

    current_time = datetime.datetime.now().time()
    print 'Data download start time:' + str(current_time)

    dat = data.MarketData()
    #dat.store_hdf5(download_sym, '08/01/2015', '08/08/2015')
    dat.store_hdf5(download_sym, '02/01/2014', '08/13/2015')

    current_time = datetime.datetime.now().time()
    print 'Data download end time:' + str(current_time)

