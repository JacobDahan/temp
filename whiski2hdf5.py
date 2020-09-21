from builtins import zip
from builtins import map
from builtins import range
from builtins import object
import os
import numpy as np
import subprocess
import multiprocessing
import tables
try:
    from whisk.python import trace
    from whisk.python.traj import MeasurementsTable
except ImportError:
    print("cannot import whisk")
import pandas
import my
import scipy.io
import ctypes
import glob
import time
import shutil
import itertools

def append_whiskers_to_hdf5(whisk_filename, h5_filename, chunk_start, measurements_filename=None):
    """Load data from whisk_file and put it into an hdf5 file
    
    The HDF5 file will have two basic components:
        /summary : A table with the following columns:
            time, id, fol_x, fol_y, tip_x, tip_y, pixlen
            These are all directly taken from the whisk file
        /pixels_x : A vlarray of the same length as summary but with the
            entire array of x-coordinates of each segment.
        /pixels_y : Same but for y-coordinates
    """
    ## Load it, so we know what expectedrows is
    # This loads all whisker info into C data types
    # wv is like an array of trace.LP_cWhisker_Seg
    # Each entry is a trace.cWhisker_Seg and can be converted to
    # a python object via: wseg = trace.Whisker_Seg(wv[idx])
    # The python object responds to .time and .id (integers) and .x and .y (numpy
    # float arrays).
    #wv, nwhisk = trace.Debug_Load_Whiskers(whisk_filename)
    print(whisk_filename)
    
    whiskers = trace.Load_Whiskers(whisk_filename)
    nwhisk = np.sum(list(map(len, list(whiskers.values()))))

    if measurements_filename is not None:
        print(measurements_filename)
        M = MeasurementsTable(str(measurements_filename))
        measurements = M.asarray()
        measurements_idx = 0

    # Open file
    h5file = tables.open_file(h5_filename, mode="a")

    ## Iterate over rows and store
    table = h5file.get_node('/summary')
    h5seg = table.row
    xpixels_vlarray = h5file.get_node('/pixels_x')
    ypixels_vlarray = h5file.get_node('/pixels_y')
    for frame, frame_whiskers in whiskers.items():
        for whisker_id, wseg in frame_whiskers.items():
            # Write to the table
            h5seg['chunk_start'] = chunk_start
            h5seg['time'] = wseg.time + chunk_start
            h5seg['id'] = wseg.id
            h5seg['fol_x'] = wseg.x[0]
            h5seg['fol_y'] = wseg.y[0]
            h5seg['tip_x'] = wseg.x[-1]
            h5seg['tip_y'] = wseg.y[-1]

            if measurements_filename is not None:
                h5seg['length'] = measurements[measurements_idx][3]
                h5seg['score'] = measurements[measurements_idx][4]
                h5seg['angle'] = measurements[measurements_idx][5]
                h5seg['curvature'] = measurements[measurements_idx][6]
                h5seg['pixlen'] = len(wseg.x)
                h5seg['fol_x'] = measurements[measurements_idx][7] 
                h5seg['fol_y'] = measurements[measurements_idx][8]
                h5seg['tip_x'] = measurements[measurements_idx][9]
                h5seg['tip_y'] = measurements[measurements_idx][10]


                measurements_idx += 1
           
            assert len(wseg.x) == len(wseg.y)
            h5seg.append()
            
            # Write x
            xpixels_vlarray.append(wseg.x)
            ypixels_vlarray.append(wseg.y)
    

    table.flush()
    h5file.close()
