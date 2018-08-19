import dask.array as da
from dask.distributed import Client


def connnect_glue():
    client = Client('128.104.222.104:8786')
    client.restart()
    x = da.from_zarr('/mnt/cephfs/smltar_numpyarr/zarr_data_full')
    print(x)
    # y = x[0:100]
    # z = x[100:200]
    # m = x[1000:1100]
    # n = x[1500:1600]
    # p = x[1400:1500]
    # zc = x[108:208]
    # mc = x[1008:1108]
    # nc = x[1508:1608]
    # pc = x[1601:1701]
    #
    # sum = (y + z - m + p) * n
    #
    # sum2 = (zc + mc + nc +pc)*sum
    #
    # sum3 = sum2 + (zc + mc + nc +pc)*sum
    #
    #
    #
    # print(sum2)
    # sum.visualize('sum3')
    # r = client.compute(sum3)
    # p = r.result()
    # print(type(p))
    # return p
    # p = r.result()
    # print(p)
    # return p

re = connnect_glue()
print(re)