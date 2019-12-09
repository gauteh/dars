import pytest
from netCDF4 import Dataset
import numpy as np

def test_dars_testData_z_sfc(benchmark):
  def k():
    d = Dataset("http://localhost:8001/data/testData.nc")
    sst = d.variables['Z_sfc'][:]

  benchmark(k)

def test_thredds_testData_z_sfc(benchmark):
  def k():
    d = Dataset("http://localhost:8002/thredds/dodsC/test/testData.nc")
    sst = d.variables['Z_sfc'][:]

  benchmark(k)

