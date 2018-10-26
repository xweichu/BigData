Just run the command to start Glue: python GlueLoader.py
Maybe some dependencies are required.
Glue connects to the Dask cluster(which is also a Ceph cluster) at Client('128.104.222.103:8786')
Visit http://128.104.222.103:8787/status to check the daskboard of Dask and tasks which are being processed.
You will see a gray picture once visualized in Glue. Just adjust the limit:min/max to something like 95% to see something different. 
