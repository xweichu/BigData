from glue.utils import view_shape
from dask.distributed import Client


shape = (512,512,512)
client = Client('10.0.1.9:8786')

def calc_random(view):
    import numpy as np
    from glue.utils import view_shape
    return np.random.random(view_shape((512,512,512), view))

def get_data(client,view):
    future = client.submit(calc_random, view)
    result = future.result()
    print(type(result))
    return result

res = get_data(client,(3,3))
print(res)
