import pytest
from netCDF4 import Dataset

def test_load_dataset():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")

def test_fetch_variables():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  print(d.variables)

def test_fetch_time_variable():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  t = d.variables['TIME']

def test_slice_time_variable():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  t = d.variables['TIME'][1:5]
  print (t)

def test_grid_var():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  sst = d.variables['SST']

def test_grid_var_read():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  sst = d.variables['SST']

  v = sst[:]
  print(v)

def test_grid_index():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  sst = d.variables['SST']
  v = sst[:].sum()
  print(v)

def test_error():
  d = Dataset("http://localhost:8001/data/coads_climatology.nc4")
  sst = d.variables['SST']
  with pytest.raises(IndexError):
    v = sst[40,:,:].sum()
    print(v)

