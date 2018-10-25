from glob import glob
import os
from dask.distributed import Client
import time


def convert_to_nparray(filename, dst_file):
    import yt
    import numpy
    # import dask.array as da
    # import zarr
    # import os
    ds = yt.load(filename)
    all_data_level_0 = ds.covering_grid(level=1, left_edge=ds.domain_left_edge, dims=ds.domain_dimensions)
    arr = all_data_level_0['density']
    numpy.save(dst_file, arr)
    # arr = numpy.load(dst_file+'.npy')
    # x = da.from_array(arr, chunks=(64, 64, 64))
    # os.mkdir(dst_file)
    # da.to_zarr(x, dst_file)

    return filename


def load_data():
    client = Client('128.104.222.104:8786')
    # i = 0
    ls = []
    for symbol in os.listdir('/mnt/cephfs/smltar/'):
        dirname =  '/mnt/cephfs/smltar/' + symbol
        dirname = dirname.split(',')
        data = client.submit(convert_to_nparray, dirname[0], '/mnt/cephfs/smltar_numpyarr_2/' + symbol)
        # ls.append(data)
        print(data.result())
        # if i>5:
        #      break
        # i = i+1
    # client.compute(ls)
    # for item in ls:
    #     yt_result = client.compute(item)
    #     print(yt_result)

load_data()