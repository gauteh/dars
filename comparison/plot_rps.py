import os, sys

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mplcyberpunk

plt.style.use('cyberpunk')
plt.rcParams['font.family'] = 'Roboto'
plt.rcParams['font.weight'] = 'regular'
plt.rcParams['font.size'] = '13'

rpsf = ['out/das_1.rps', 'out/dds_1.rps', 'out/dods_1_small.rps', 'out/dods_1_big.rps', 'out/dods_2.rps' ]

data = []
for f in rpsf:
  print("reading ", f, "..")
  with open(f) as fd:
    d = [float(dd) for dd in fd.readlines()]
    data.append(d)

data = np.array(data)

xlabels = ['Dars', 'Thredds', 'Hyrax' ]
cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

# from: https://matplotlib.org/3.2.1/gallery/lines_bars_and_markers/barchart.html#sphx-glr-gallery-lines-bars-and-markers-barchart-py
def autolabel(ax, rects):
  """Attach a text label above each bar in *rects*, displaying its height."""
  for rect in rects:
    height = rect.get_height()
    ax.annotate('{}'.format(height),
        xy=(rect.get_x() + rect.get_width() / 2, height),
        xytext=(0, 3),  # 3 points vertical offset
        textcoords="offset points",
        ha='center', va='bottom')

aph = .8

fig, (ax1, ax2) = plt.subplots(1, 2, figsize = (12,8))
fig2, (ax3, ax4, ax5) = plt.subplots(1, 3, figsize = (12,8))

## DAS
r1 = ax1.bar(xlabels, data[0,:], color = cycle, alpha = aph)
ax1.set_title('Metadata (DAS)')
ax1.set_ylabel('Requests / Sec')
autolabel(ax1, r1)

## DDS
r2 = ax2.bar(xlabels, data[1,:], color = cycle, alpha = aph)
ax2.set_title('Metadata (DDS)')
ax2.set_ylabel('Requests / Sec')
autolabel(ax2, r2)

## DODS1 small
r3 = ax3.bar(xlabels, data[2,:], color = cycle, alpha = aph)
ax3.set_title('Data (40kb, slicing large dataset)')
ax3.set_ylabel('Requests / Sec')
autolabel(ax3, r3)

## DODS1 big
r4 = ax4.bar(xlabels, data[3,:], color = cycle, alpha = aph)
ax4.set_title('Data (464mb, entire large dataset)')
ax4.set_ylabel('Requests / Sec')
autolabel(ax4, r4)

## DODS2 big
r5 = ax5.bar(xlabels, data[4,:], color = cycle, alpha = aph)
ax5.set_title('Data (759kb, entire small dataset)')
ax5.set_ylabel('Requests / Sec')
autolabel(ax5, r5)


mplcyberpunk.add_glow_effects()
fig.suptitle('Requests per second (2 threads with 10 concurrent connections)')
fig2.suptitle('Requests per second (2 threads with 10 concurrent connections)')

fig.savefig('rps_meta.png')
fig2.savefig('rps_data.png')
plt.show()


