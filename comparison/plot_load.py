import os, sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mplcyberpunk

plt.style.use('cyberpunk')
plt.rcParams['font.family'] = 'Roboto'
plt.rcParams['font.weight'] = 'regular'
plt.rcParams['font.size'] = '13'

data = pd.read_csv('./server_load.csv', header = 0)
print(data)

cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

# from: https://matplotlib.org/3.2.1/gallery/lines_bars_and_markers/barchart.html#sphx-glr-gallery-lines-bars-and-markers-barchart-py
def autolabel(ax, rects, suffix):
  """Attach a text label above each bar in *rects*, displaying its height."""
  for rect in rects:
    height = rect.get_height()
    ax.annotate('{} {}'.format(height, suffix),
        xy=(rect.get_x() + rect.get_width() / 2, height),
        xytext=(0, 3),  # 3 points vertical offset
        textcoords="offset points",
        ha='center', va='bottom')

aph = .8
aph2 = .5

def plot_case(ax, case, title):
  x = np.arange(0,3)
  width = .35

  xlabels = data[data['case'] == case]['server']
  cpu = data[data['case'] == case]['cpu']
  memory = data[data['case'] == case]['memory']

  memax = ax.twinx()
  r1 = ax.bar(x - width/2 - .05, cpu, color = cycle, alpha = aph, width = width)
  r2 = memax.bar(x + width/2 + .05, memory, color = cycle, alpha = aph2, width = width)

  ax.set_title(title)
  autolabel(ax, r1, '%')
  autolabel(memax, r2, 'mb')

  ax.set_ylabel('CPU [%]')
  memax.set_ylabel('Memory [mb]')
  memax.grid(False)
  memax.set_xticks(x)
  memax.set_xticklabels(xlabels)

  ax.set_ylim([0, 850])
  memax.set_ylim([0, 11000])

fig, (ax1, ax2) = plt.subplots(1, 2, figsize = (20,8))
fig2, (ax3, ax4, ax5) = plt.subplots(1, 3, figsize = (20,8))

## Plot cases
plot_case(ax1, 'das1', 'Metadata (DAS)')
plot_case(ax2, 'dds1', 'Metadata (DDS)')
plot_case(ax3, 'dods1s', 'Data (40kb, slicing large dataset)')
plot_case(ax4, 'dods1b', 'Data (464mb, entire large dataset)')
plot_case(ax5, 'dods2', 'Data (759kb, entire small dataset)')


mplcyberpunk.add_glow_effects()
fig.suptitle('CPU and memory usage during load testing')
fig2.suptitle('CPU and memory usage during load testing')

fig.savefig('load_meta.png')
fig2.savefig('load_data.png')
plt.show()



