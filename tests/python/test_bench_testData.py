import pytest
from netCDF4 import Dataset
import numpy as np
from . import *

def test_dars_testData_z_sfc(dars, benchmark):
  def k():
    d = Dataset(dars + "coads_climatology.nc")
    sst = d.variables['SST'][:]

  benchmark(k)

def test_thredds_testData_z_sfc(tds, benchmark):
  def k():
    d = Dataset(tds + "coads_climatology.nc")
    sst = d.variables['SST'][:]

  benchmark(k)

def test_hyrax_testData_z_sfc(hyrax, benchmark):
  def k():
    d = Dataset(hyrax + "coads_climatology.nc")
    sst = d.variables['SST'][:]

  benchmark(k)

