#!/usr/bin/env python

import numpy as np
import matplotlib as mpl
#mpl.use('Agg')
import matplotlib.pyplot as plt
import pylab as P
import sys
#for camera ready
#plt.rcParams['ps.useafm'] = True
#plt.rcParams['pdf.use14corefonts'] = True
#plt.rcParams['text.usetex'] = True

P.rcParams['legend.loc'] = 'best'
SHOW_GRAPH=False

def plot_throughput(x , y1, y2, y1err, y2err):
    P.figure()
    width=0.125
    #P.bar(x, YData_latency_noloc_mean, width, color='#ff6666', yerr=YData_latency_noloc_std,  label="no locality")
    #P.bar(x + width, YData_latency_withloc_mean, width, color='#66d9d9', yerr=YData_latency_withloc_std, label="with locality")
    P.bar(x, y1,
          width, color='#ff6666',
          yerr=y1err, ecolor='red',
          label="with locality")
    P.bar(x + width, y2,
          width, color='#66d9d9',
          yerr=y2err, ecolor='red',
          label="no locality")
    P.xticks(x + width / 2, ["0.1MB", "1MB", "10MB", "100MB"], size=14)
    P.xlabel("object size", {'fontsize': 18, 'color' : 'k'})
    P.ylabel("task throughput (tasks/sec)", {'fontsize': 18, 'color' : 'k'})
    P.legend(prop={'size' : 18})
    P.grid()
    P.savefig("locality-throughput.png", bbox_inches = 'tight', pad_inches = 0.1)

def plot_latency(x , y1, y2, y1err, y2err):
    # printx
    # print"y1", y1
    # print"y2", y2
    # print"y1s", y1err
    # print"y2s", y2err

    P.figure()
    width=0.125
    P.bar(x, y1,
          width, color='#ff6666',
          yerr=y1err, ecolor='red',
          label="locality-aware")
    P.bar(x + width, y2,
          width, color='#66d9d9',
          yerr=y2err, ecolor='red',
          label="locality-unaware")
    P.xticks(x + width / 2, ["0.1MB", "1MB", "10MB", "100MB"], size=14)
    P.xlabel("object size", {'fontsize': 18, 'color' : 'k'})
    P.ylabel("average task latency(s)", {'fontsize': 18, 'color' : 'k'})
    P.yscale('log')
    P.legend(prop={'size' : 18})
    P.grid()
    P.savefig("locality-latency.png", bbox_inches = 'tight', pad_inches = 0.1)

#numtasks = [200000, 20000, 2000]
#100GB results, varying number of tasks, constant data size
#YData_latency_withloc = [
#[16.728374004364014, 17.93216323852539, 18.260265350341797],
#[1.3860249519348145, 1.4007246494293213, 1.2912530899047852],
#[0.1918330192565918, 0.205596923828125, 0.23424220085144043] ]
#
#YData_latency_noloc = [
#[280.98607087135315],
#[45.194687843322754, 42.63055777549744, 47.12190270423889],
#[40.26934766769409, 35.33686828613281, 35.652066469192505] ]

numtasks = [1000, 1000, 1000]#, 1000]
YData_latency_withloc = [
[0.1757664680480957, 0.18332195281982422, 0.19823527336120605],
[0.1822972297668457, 0.22429585456848145, 0.17738056182861328],
[0.15737438201904297, 0.17440366744995117, 0.19196311950683594],
#[0.10251259803771973, 0.09792780876159668]
]
YData_latency_noloc = [
[0.17428016662597656, 0.1109615135192871, 0.26913537979125977],
[0.31075258255004883, 0.28856120109558105, 0.4220619201660156],
[0.48357152938842773, 0.5545780658721924, 0.8652563095092773],
#[36.68243169784546, 33.7419695854187, 35.90411734580994]
]

YData_latency_withloc_mean = [ np.mean(lst) for lst in YData_latency_withloc ]
YData_latency_withloc_std  = [ np.std(lst) for lst in YData_latency_withloc ]
YData_latency_noloc_mean   = [ np.mean(lst) for lst in YData_latency_noloc ]
YData_latency_noloc_std    = [ np.std(lst) for lst in YData_latency_noloc ]
YData_withloc_thruput = []
for i in range(len(numtasks)):
  YData_withloc_thruput.append(numtasks[i]/np.array(YData_latency_withloc[i]))

YData_noloc_thruput = []
for i in range(len(numtasks)):
  YData_noloc_thruput.append(numtasks[i]/np.array(YData_latency_noloc[i]))

YData_withloc_thruput_mean = [ np.mean(a) for a in YData_withloc_thruput ]
YData_withloc_thruput_std = [ np.std(a) for a in YData_withloc_thruput ]
YData_noloc_thruput_std = [ np.std(a) for a in YData_noloc_thruput ]
YData_noloc_thruput_mean = [ np.mean(a) for a in YData_noloc_thruput ]

X = np.arange(len(YData_latency_noloc_mean))/2.0
plot_throughput(X, YData_withloc_thruput_mean, YData_noloc_thruput_mean, YData_withloc_thruput_std, YData_noloc_thruput_std) 

y1 = [np.mean(1/a) for a in YData_withloc_thruput]
y2 = [np.mean(1/a) for a in YData_noloc_thruput]
y1err = [np.std(1/a) for a in YData_withloc_thruput]
y2err = [np.std(1/a) for a in YData_noloc_thruput]

plot_latency(X, y1, y2, y1err, y2err)

if SHOW_GRAPH:
    P.show()

sys.exit(0)

#convert times to BW and use them to calculate Y error for error bars
YDataBW = [x/np.array(ly)  for x,ly in zip(numtasks,YData)]
Y = [ np.median(arr) for arr in YDataBW ] #Y units : tasks/sec
#Y = [ y/10**6 for y in Y] # Y units: Mtasks/sec
# print"Y ", Y
YErr = map(lambda arr: np.median([np.abs(np.median(arr) - x) for x in arr ]), YDataBW)
# print"YErr", YErr

P.figure()
plt.bar(X, Y, align='center',
        linewidth= 0, edgecolor = 'k',
        yerr = YErr, ecolor='red', width=8) 
P.ylim([0, max(Y)+10])
P.xlim([0, max(X)+5])
P.xticks(numnodes, size=14)
P.yticks(np.arange(0, max(Y)+10, 10**5), np.arange(0, max(Y)+10, 10**5)/10**6, size=14)
P.xlabel("number of nodes", {'fontsize': 18, 'color' : 'k'})
P.ylabel("tasks per second (millions)", {'fontsize': 18, 'color' : 'k'})
P.grid(True)
