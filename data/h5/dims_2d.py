from h5py import File
import numpy as np

# http://docs.h5py.org/en/stable/high/dims.html

f = File('dims_2d.h5', 'w')

f['x1'] = [1, 2]
f['x1'].make_scale('x1 name')

f['y1'] = [1, 2, 3]
f['y1'].make_scale('y1 name')
f['data'] = np.ones((2,3), 'f')

f['data'].dims[0].attach_scale(f['x1'])
f['data'].dims[1].attach_scale(f['y1'])

