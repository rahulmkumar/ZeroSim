import pandas as pd
import numpy as np
import h5py as h



def main():
    mydf = pd.read_csv('./debug/df_SMA_stats.csv')

    #store = pd.HDFStore('mydata.h5',mode='a')
    store = pd.HDFStore('mydata.h5',mode='a', complevel = 9, complib = 'zlib')
    #store._complevel = 9
    #store._complib = 'zlib'
    #store._mode = 'r+'
    
    #store.copy('mydata.h5', mode='r+', propindexes=True, keys=None, complib='zlib', complevel=9, fletcher32=False, overwrite=True)
    store['A'] = mydf
    store.close()

   
    #mydf.to_hdf('mydata3.h5','B')
    #mydf._m
    
    
    #f = h.File("testfile.hdf5")
    #f["High"] = mydf['high'].ix[:]
    
    #dset = f["my dataset"]
    
    
    


if __name__ == '__main__':
    main()  