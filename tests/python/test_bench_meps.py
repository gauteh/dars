import pytest
from netCDF4 import Dataset
import numpy as np
from . import *

def test_file_meps(dars, benchmark):
  def k():
    d = Dataset("data/meps_det_vc_2_5km_latest.nc")
    sst = d.variables['x_wind_ml'][1:1000]

  benchmark(k)

def test_dars_meps(dars, benchmark):
  def k():
    d = Dataset(dars + "meps_det_vc_2_5km_latest.nc")
    sst = d.variables['x_wind_ml'][1:1000]

  benchmark(k)

def test_thredds_meps(tds, benchmark):
  def k():
    d = Dataset(tds + "meps_det_vc_2_5km_latest.nc")
    sst = d.variables['x_wind_ml'][1:1000]

  benchmark(k)


