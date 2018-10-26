import numpy as np
from glue.core.component_id import ComponentID
from glue.core.data import BaseCartesianData
from glue.utils import view_shape
from dask.distributed import Client
from glue.core import DataCollection
from glue.app.qt.application import GlueApplication

class DaskData(BaseCartesianData):

    def __init__(self, client):
        super(DaskData, self).__init__()
        self.data_cid = ComponentID(label='data', parent=self)
        self.client = client

    @property
    def label(self):
        return "Dask Data"
    
    @property
    def shape(self):
        return (2560, 2560, 2560)

    @property
    def main_components(self):
        return [self.data_cid]

    def get_kind(self, cid):
        return 'numerical'
    
    @property
    def ndim(self):
        return len(self.shape)

    def get_data(self, cid, view=None):
        from dask.distributed import Client
        import dask.delayed
        import dask.array as da
        client = self.client

        def load_ceph_data(view):
            import dask.array as da
            import numpy as np
            from glue.utils import view_shape
            x = da.from_zarr('/mnt/cephfs/zarr_data_full')
            f = 0
            scale = 10

            lh = []
            for k in range(scale):
                lc = []
                for i in range(scale):
                    lr = []
                    for j in range(scale):
                        lr.append(x[f%3500])
                        f = f+1
                    lc.append(da.concatenate(lr))
                lh.append(da.concatenate(lc,1))
            z = da.concatenate(lh,2)

            if view != None:
                z = z[view]

            z = z.compute()
            return z

        future = client.submit(load_ceph_data, view)
        result = future.result()
        return result 

    def get_mask(self, subset_state, view=None):
        print("get mask triggered")
        return subset_state.to_mask(self, view=view)


    # def compute_statistic(self, statistic, cid,
    #                       axis=None, finite=True,
    #                       positive=False, subset_state=None,
    #                       percentile=None, random_subset=None):

    #     if statistic == 'minimum':
    #         return 0 #da.min(z,axis).compute()
    #     elif statistic == 'maximum':
    #         return 1 #da.max(z,axis).compute()
    #     elif statistic == 'mean' or statistic == 'median':
    #         return 0.5 #da.mean(z,axis).compute()
    #     elif statistic == 'percentile':
    #         return percentile/100
    #         # return da.percentile(z,percentile)
    #     elif statistic == 'sum':
    #         return self.size/2 #da.sum(z.axis).compute()

    #         # final_shape = tuple(self.shape[i] for i in range(self.ndim)
    #         #                     if i not in axis)
    #         # return np.random.random(final_shape)
    #         # return 0
    #     return 0

    def compute_statistic(self, statistic, cid,
                          axis=None, finite=True,
                          positive=False, subset_state=None,
                          percentile=None, random_subset=None):


        from dask.distributed import Client
        import dask.delayed
        import dask.array as da
        client = self.client

        def load_data(statistic, axis):
            import dask.array as da
            import numpy as np
            from glue.utils import view_shape
            x = da.from_zarr('/mnt/cephfs/zarr_data_full')
            f = 0
            scale = 10

            lh = []
            for k in range(scale):
                lc = []
                for i in range(scale):
                    lr = []
                    for j in range(scale):
                        lr.append(x[f%3500])
                        f = f+1
                    lc.append(da.concatenate(lr))
                lh.append(da.concatenate(lc,1))
            z = da.concatenate(lh,2)


            if statistic == 'minimum':
                return da.min(z,axis).compute()
            elif statistic == 'maximum':
                return da.max(z,axis).compute()
            elif statistic == 'mean' or statistic == 'median':
                return da.mean(z,axis).compute()
            elif statistic == 'percentile':
                return percentile/100
            elif statistic == 'sum':
                return da.sum(z.axis).compute()
            return 0
        
        future = client.submit(load_data, statistic,axis)
        result = future.result()
        print("compute statistics function triggered")
        return result     


    def compute_histogram(self, cids,
                          range=None, bins=None, log=False,
                          subset_state=None, subset_group=None):
        
        from dask.distributed import Client
        import dask.delayed
        import dask.array as da
        client = self.client

        def get_histogram():
            import dask.array as da
            import numpy as np
            x = da.from_zarr('/mnt/cephfs/zarr_data_full')
            f = 0
            scale = 10
            rg = [0,1,2,3,4,5,6,7,8,9]
            lh = []
            for k in rg:
                lc = []
                for i in rg:
                    lr = []
                    for j in rg:
                        lr.append(x[f%3500])
                        f = f+1
                    lc.append(da.concatenate(lr))
                lh.append(da.concatenate(lc,1))
            z = da.concatenate(lh,2)
            h , bins = da.histogram(z,bins=np.arange(100))
            h = h.compute()
            return h
        
        future = client.submit(get_histogram)
        result = future.result()
        return result


client = Client('128.104.222.103:8786')
d = DaskData(client)
dc = DataCollection([d])
ga = GlueApplication(dc)
ga.start(maximized=False)