#!/usr/bin/env python

import numpy as np
import sys, os.path
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import math

if len(sys.argv) < 2:
    print("usage:", sys.argv[0], " <input> [output]")
    sys.exit(1)

input_filename = sys.argv[1];
if len(sys.argv) >= 3:
    output_filename = sys.argv[2]
else:
    basename, ext = os.path.splitext(os.path.basename(input_filename))
    output_filename = basename + ".pdf"

fin = open(input_filename, "r")
header = fin.readline().rstrip('\n\r').split('\t')
rows = list(map(lambda line: line.rstrip('\n\r').split('\t'), fin.readlines()))
columns = []
for i in range(len(header)):
    columns.append(list(map(lambda row: eval(row[i]), rows)))

window_size = 8
moving_avgs = []
for i in range(len(columns)):
    moving_avgs.append([0] * len(rows))
for j in range(len(rows)):
    for i in range(1, len(header)):
        if j >= window_size:
            moving_avgs[i][j] = int(round(1.0 * 
                (columns[i][j] - columns[i][j - window_size]) / (window_size * 0.25)))
        else:
            moving_avgs[i][j] = int(round(1.0 * columns[i][j] / ((j + 1) * 0.25)))

fig, ax = plt.subplots(figsize=(8, 4.5))

#sinlj_t_line = plt.plot(
#    window_size[:len(sinlj_t)], sinlj_t,
#    '-v', linewidth=2.0, color='#ca4864',
#    label='SINLJ Tumbling', markersize=10)[0]

handles = [] 

for i in range(1, len(header)):
    handle = plt.plot(
        columns[0], moving_avgs[i], linewidth=2.0,
        label=header[i])[0]
    handles.append(handle)

lgd = ax.legend(
        handles,
        [h.get_label() for h in handles],
        ncol=2, fontsize=18, loc="upper center",
        bbox_to_anchor=(0.5, 1.30))

ax.set_ylabel('counters', fontsize=24)
ax.set_xlabel(header[0], fontsize=24)
plt.xticks(fontsize=22)
plt.yticks(fontsize=22)

plt.savefig(output_filename, format="pdf",
        bbox_extra_artists=(lgd,), bbox_inches='tight')

