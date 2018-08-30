import dask.array as da
from dask.distributed import Client
# import os
# import subprocess
# import time



def connnect_glue():
    # os.system('dask-ssh 128.104.222.{103,104,105,107}')
    # subprocess.call('dask-ssh', '128.104.222.{103,104,105,107}')
    # time.sleep(10)
    import numpy as np

    client = Client('128.104.222.103:8786')
    client.restart()
    x = da.from_zarr('/mnt/cephfs/smltar_numpyarr/zarr_data_full')
    print(x)
    # y = x[0:1]
    # z = x[100:101]
    # m = x[1000:1001]
    # n = x[1500:1501]
    # p = x[1400:1401]

    y = x[0:30]
    z = x[100:130]
    m = x[1000:1030]
    n = x[1500:1530]
    p = x[1400:1430]

    # zc = x[108:208]
    # mc = x[1008:1108]
    # nc = x[1508:1608]
    # pc = x[1601:1701]
    #
    sum = (y + z - m + p) * n
    #
    # sum2 = (zc + mc + nc +pc)*sum
    #
    # sum3 = sum2 + (zc + mc + nc +pc)*sum
    #
    #
    #
    # print(sum2)
    # sum.visualize('sum3')

    # frm = sum[15]
    fu = client.compute(sum)
    # p = r.result()
    # print(type(p))
    # return p
    re = fu.result()

    re = np.array(re)
    np.save("/mnt/cephfs/result/test", re[15])

    # print(p)
    return re[15]

re = connnect_glue()
print(re)