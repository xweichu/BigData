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
        return "Ceph Data"
    
    @property
    def shape(self):
        return (5120, 5120, 5120)

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
        import dask.array as da
        client = client = Client('128.104.222.103:8786')
        print("view:")
        print(view)

        def calc_random(view):
            import dask.array as da
            import numpy as np
            from glue.utils import view_shape
            x = da.from_zarr('/mnt/cephfs/zarr_data_full')
            z = x[2300]
            z = z[view]
            z = z.compute()
            return z

        def load_ceph_data(view):
            import dask.array as da
            import numpy as np
            from glue.utils import view_shape
            x = da.from_zarr('/mnt/cephfs/zarr_data_full')

            # x1 = x[300]
            # x2 = x[1300]
            # x3 = x[2300]
            # x4 = x[3300]
            # x5 = x[320]
            # x6 = x[1320]
            # x7 = x[2320]
            # x8 = x[3320]

            # x11 = x[380]
            # x21 = x[1380]
            # x31 = x[2380]
            # x41 = x[3380]
            # x51 = x[360]
            # x61 = x[1480]
            # x71 = x[2460]
            # x81 = x[3460]


            # first try
            # z1 = da.block([x1, x2, x3, x4],[x5,x6,x7,x8])
            # z2 = da.block([x1,x2, x3, x4],[x1,x2, x3, x4])
            # z = da.block(z1,z2)

            #second try
            ## l1 = []
            ## l1.append(x1)
            ## l1.append(x2)
            ## l1.append(x3)
            ## l1.append(x4)

            # pc1 = [x1, x2, x3, x4]
            # pc1 = da.concatenate(l1)

            # pc2 = [x5, x6, x7, x8]
            # pc2 = da.concatenate(pc2)

            # pc3 = [x11, x21, x31, x41]
            # pc3 = da.concatenate(pc3)
            
            # pc4 = [x51, x61, x71, x81]
            # pc4 = da.concatenate(pc4)

            # z = da.concatenate([pc1,pc2,pc3,pc4],1)
            # z = da.concatenate([z,z,z,z],2)

            # Create large dataset:
          

            f = 0
            scale = 20

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

            # # uncomment to switch on massive computation that leverages all the resources of the workers.
            # y = x[0:30]
            # f = x[100:130]
            # m = x[1000:1030]
            # n = x[1500:1530]
            # p = x[1400:1430]
            # sum = (y + f - m + p) * n
            # sum.compute()

            if view != None:
                z = z[view]

            z = z.compute()
            return z

        # client.restart()
        future = client.submit(load_ceph_data, view)
        result = future.result()
        print("function triggered")
        return result 

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

    def compute_histogram(self, cids,
                          range=None, bins=None, log=False,
                          subset_state=None, subset_group=None):
        
        import numpy as np
        
        data = self.get_data(cids[0])

        # result = np.histogram2d
        
        # from dask.distributed import Client
        # import dask.delayed
        # import dask.array as da
        # import numpy as np
        # client = client = Client('128.104.222.103:8786')

        # def get_histogram():
        #     import dask.array as da
        #     import numpy as np
        #     from glue.utils import view_shape
        #     x = da.from_zarr('/mnt/cephfs/zarr_data_full')
        #     f = 0
        #     scale = 2
        #     lh = []
        #     for k in range(scale):
        #         lc = []
        #         for i in range(scale):
        #             lr = []
        #             for j in range(scale):
        #                 lr.append(x[f%3500])
        #                 f = f+1
        #             lc.append(da.concatenate(lr))
        #         lh.append(da.concatenate(lc,1))
        #     z = da.concatenate(lh,2)
        #     z = z.compute()

        # future = client.submit(get_histogram)
        # result = future.result()
        # result = np.histogram2d(result)
        # print("function triggered")
        return result


# We now create a data object using the above class,
# and launch a a glue session

from glue.core import DataCollection
from glue.app.qt.application import GlueApplication
# client = Client('10.0.1.9:8786')

d = RandomData()
dc = DataCollection([d])
ga = GlueApplication(dc)
ga.start(maximized=False)