import pytest
from netCDF4 import Dataset
import numpy as np
from . import *

def test_dars_testData_z_sfc(dars, benchmark):
  def k():
    d = Dataset(dars + "meps_det_vc_2_5km_latest.nc")
    sst = d.variables['x_wind_ml'][1:1000]

  benchmark(k)

def test_thredds_testData_z_sfc(tds, benchmark):
  def k():
    d = Dataset(tds + "meps_det_vc_2_5km_latest.nc")
    sst = d.variables['x_wind_ml'][1:1000]

  benchmark(k)


