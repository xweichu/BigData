from dask import delayed
import os
import numpy
import dask.array as da
# import dask
# import zarr
# from dask.distributed import Client

def merge_arrays(data_path):
    numpyload = delayed(numpy.load, pure=True)

    filelist = os.listdir(data_path)

    def filenum(x):
        return int(x[8:-4])

    filelist = sorted(filelist, key=filenum)

    array_list = []

    for symbol in filelist:
        arr_name = data_path+symbol
        arr_d = numpyload(arr_name)
        arr = da.from_delayed(arr_d, (256, 256, 256), float)
        array_list.append(arr)
        print(arr_name)


    filelist = os.listdir(data_path[:-1]+"_2")
    filelist = sorted(filelist, key=filenum)

    for symbol in filelist:
        arr_name = data_path+symbol
        arr_d = numpyload(arr_name)
        arr = da.from_delayed(arr_d, (256, 256, 256), float)
        array_list.append(arr)
        print(arr_name)

    z = da.stack(array_list)



    z.rechunk((256, 256, 256, 1))
    # da.to_npy_stack(data_path+'zarr_data', z)
    # m = z[:][:][:][199]
    # client = Client('128.104.222.103:8786')
    # re = client.compute(m)
    da.to_zarr(z, data_path+'zarr_data_full')

    # print(m)


    # z.persist()

# client = Client('128.104.222.103:8786')
#     # client.scatter(z)
#     # client.persist(z)
# re = client.submit(merge_arrays,'/mnt/cephfs/smltar_numpyarr/')
# re.result()
merge_arrays('/mnt/cephfs/smltar_numpyarr/')