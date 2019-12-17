from . import *
from netCDF4 import Dataset

def test_aggregate(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][:])

def test_aggregate_slice(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][0:50])

def test_aggregate_slice_offset(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['time'][5:50])

def test_aggregate_slice_offset_t(dars):
  d = Dataset(dars + "ncml/aggExisting.ncml")
  print (d['P'][:,:,:])

  d = Dataset(dars + "ncml/feb.nc")
  print (d['P'][:,:,:])

  # d = Dataset("data/ncml/feb.nc")
  # print (d['P'][:,:,:])

