from . import *
import numpy as np
from netCDF4 import Dataset

# jan, t 0..31
# feb, t 32..(31+28)

def test_aggregate(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][:])

def test_aggregate_slice(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][0:50])

def test_aggregate_slice_offset(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][5:50])

def test_aggregate_slice_offset_t(dars, data):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  ap = d['P'][3:40,:,:]

  d = Dataset(data + "ncml/jan.nc")
  dp = d['P'][3:,:,:]

  np.testing.assert_array_equal(ap[:31-3,:,:], dp[:,:,:])

