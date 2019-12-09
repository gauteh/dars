from netCDF4 import Dataset
import numpy as np

def test_thredds_dars_testData():
  d = Dataset("http://localhost:8001/data/testData.nc")
  t = Dataset("http://localhost:8002/thredds/dodsC/test/testData.nc")

  for var in d.variables:
    print("testing:", var)
    da = d.variables[var][:]
    dt = t.variables[var][:]

    assert np.array_equal(da, dt)

