import numpy as np

from glue.core.component_id import ComponentID
from glue.core.data import BaseCartesianData
from glue.utils import view_shape


class RandomData(BaseCartesianData):

    def __init__(self):
        super(RandomData, self).__init__()
        # self.client = client
        self.data_cid = ComponentID(label='data', parent=self)

    @property
    def label(self):
        return "Random Data"
    

    @property
    def shape(self):
        return (4096, 4096, 4096)

    @property
    def main_components(self):
        return [self.data_cid]

    def get_kind(self, cid):
        return 'numerical'
    
    

    def get_data(self, cid, view=None):
        # if cid in self.pixel_component_ids:
        #     return super(RandomData, self).get_data(cid, view=view)
        # else:
        from dask.distributed import Client
        import dask.delayed

        client = client = Client('128.104.222.106:8786')

        def calc_random(view):
            import dask.array as da
            import numpy as np
            from glue.utils import view_shape
            # x = da.from_zarr('/mnt/cephfs/smltar_numpyarr/zarr_data_full')
            # print(x)
            # y = x[0:30]
            # z = x[100:130]
            # m = x[1000:1030]
            # n = x[1500:1530]
            # p = x[1700:1730]

            # sum = (y + z - m + p) * n
            # print(sum)
            # fu = client.compute(sum)
            # #print(r.result())
            # re = fu.result()
            result = np.random.random(view_shape((4096,4096,4096), view))
            return result

        future = client.submit(calc_random, view)
        result = future.result()
        print("function triggered")
        return result #np.random.random(view_shape((512,512,512), view))

    def get_mask(self, subset_state, view=None):
        return subset_state.to_mask(self, view=view)

    def compute_statistic(self, statistic, cid,
                          axis=None, finite=True,
                          positive=False, subset_state=None,
                          percentile=None, random_subset=None):
        if axis is None:
            if statistic == 'minimum':
                return 0
            elif statistic == 'maximum':
                if cid in self.pixel_component_ids:
                    return self.shape[cid.axis]
                else:
                    return 1
            elif statistic == 'mean' or statistic == 'median':
                return 0.5
            elif statistic == 'percentile':
                return percentile / 100
            elif statistic == 'sum':
                return self.size / 2
        else:
            final_shape = tuple(self.shape[i] for i in range(self.ndim)
                                if i not in axis)
            return np.random.random(final_shape)

    def compute_histogram(self, cid,
                          range=None, bins=None, log=False,
                          subset_state=None, subset_group=None):
        return np.random.random(bins) * 100


# We now create a data object using the above class,
# and launch a a glue session

from glue.core import DataCollection
from glue.app.qt.application import GlueApplication
# client = Client('10.0.1.9:8786')

d = RandomData()
dc = DataCollection([d])
ga = GlueApplication(dc)
ga.start(maximized=False)