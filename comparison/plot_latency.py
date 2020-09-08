import os, sys

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mplcyberpunk
plt.style.use('cyberpunk')

latencies = sys.argv[1:]
print('latencies:', latencies)

plt.figure()
ax = plt.gca()

for l in latencies:
  with open(l) as f:
    data = f.readlines()

  start = 0
  for ll in data:
    if 'Value   Perce' in ll:
      start += 2
      break
    else:
      start += 1

  end = 0
  for ll in data:
    if '#[Mean' in ll:
      break
    else:
      end += 1

  data = data[start:end]
  data = [[float(dd.strip()) for dd in d.split(' ') if len(dd.strip()) > 0] for d in data]

  data = np.array(data)
  plt.plot(data[:,1], data[:,0], label = l)
  ax.set_xscale('logit')
  plt.xticks([0.25, 0.5, 0.9, 0.99, 0.999, 0.9999, 0.99999, 0.999999])
  majors = ["25%", "50%", "90%", "99%", "99.9%", "99.99%", "99.999%", "99.9999%"]
  ax.xaxis.set_major_formatter(ticker.FixedFormatter(majors))
  ax.xaxis.set_minor_formatter(ticker.NullFormatter())
  # print(data)

plt.ylabel('Latency [ms]')
plt.xlabel('Percentile [%]')
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102),
            loc=3, ncol=2,  borderaxespad=0.)
plt.title('30.000 req/sec with 100 connections and 12 threads over 10 seconds')
plt.show()
