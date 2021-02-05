import pytest
import numpy as np
from netCDF4 import Dataset

# local = '/lustre/storeA/project/fou/hi/legacy-ocean/thredds/SVIM/1960/ocean_avg_19600101.nc4'
# local = '/home/gauteh/dev/dars/data/met/ocean_avg_19600101.nc4'
tds = 'https://thredds.met.no/thredds/dodsC/nansen-legacy-ocean/SVIM/1960/ocean_avg_19600101.nc4'
dars = 'http://localhost:8001/data/met/svim.ncml'

def test_coordinate_vars():
  lcl = Dataset(tds)
  drs = Dataset(dars)

  tm = lcl['ocean_time'][:]
  dtm = drs['ocean_time'][:len(tm)]

  np.testing.assert_equal(tm, dtm)

  for v in lcl.dimensions:
    print('testing', v)
    if v == 'ocean_time':
      print('aggregate variable tested for first month already')
    else:
      if v in lcl.variables:
        np.testing.assert_equal(
            lcl[v][:],
            drs[v][:])
      else:
        print('%s not in variables' % v)

def test_temp():
  lcl = Dataset(tds)
  drs = Dataset(dars)

  for oti in range(4, 7):
    np.testing.assert_equal(
        lcl['temp'][oti, :, :, :],
        drs['temp'][oti, :, :, :])



