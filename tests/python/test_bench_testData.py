import pytest
from netCDF4 import Dataset
import numpy as np
from . import *

def test_dars_testData_z_sfc(dars, benchmark):
  def k():
    d = Dataset(dars + "testData.nc")
    sst = d.variables['Z_sfc'][:]

  benchmark(k)

def test_thredds_testData_z_sfc(tds, benchmark):
  def k():
    d = Dataset(tds + "testData.nc")
    sst = d.variables['Z_sfc'][:]

  benchmark(k)

