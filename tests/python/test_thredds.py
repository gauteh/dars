from netCDF4 import Dataset
import numpy as np
from . import *

def test_thredds_dars_testData(dars, tds):
  d = Dataset(dars + "testData.nc")
  t = Dataset(tds + "testData.nc")

  for var in d.variables:
    print("testing:", var)
    da = d.variables[var][:]
    dt = t.variables[var][:]

    assert np.array_equal(da, dt)

