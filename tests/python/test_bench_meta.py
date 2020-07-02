import pytest
from netCDF4 import Dataset
import numpy as np
from . import *

def test_file_metadata_z_sfc(data, benchmark):
  def k():
    d = Dataset(data + "coads_climatology.nc4")
    sst = d.variables['SST']

  benchmark(k)

def test_dars_metadata_z_sfc(dars, benchmark):
  def k():
    d = Dataset(dars + "coads_climatology.nc4")
    sst = d.variables['SST']

  benchmark(k)

def test_thredds_metadata_z_sfc(tds, benchmark):
  def k():
    d = Dataset(tds + "coads_climatology.nc4")
    sst = d.variables['SST']

  benchmark(k)

def test_hyrax_metadata_z_sfc(hyrax, benchmark):
  def k():
    d = Dataset(hyrax + "coads_climatology.nc4")
    sst = d.variables['SST']

  benchmark(k)


