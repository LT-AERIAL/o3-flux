import numpy as np
import pandas as pd
import datetime
import os
import sys
datapath_toolbox = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','zz_Python')
sys.path.insert(1, datapath_toolbox)
from Loobos_Toolbox_NewTower import *
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

if not 'TODAY' in globals():  # you can run another date by setting TODAY to False and using the time specification in the else statement.
    TODAY = True
figpath  = 'W:/ESG/DOW_MAQ/MAQ_Archive/loobos_archive/graphs/cur'
datapath = getDatapath(None)
FS       = 12

if not 'progress' in globals(): progress = list()
if not 'YESTER'   in globals(): YESTER = 0
if TODAY:
    t1      = np.datetime64('today')-np.timedelta64(YESTER,'D')
    t1      = np.datetime64('%4d-%02d-%02d 00:00:00'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    if YESTER > 0:
        t2  = t1 + np.timedelta64(1,'D')
    else:
        t2      = np.datetime64('now')
        t2      = np.datetime64('%4d-%02d-%02d %02d:00:00'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day,t2.astype(object).hour))+np.timedelta64(30,'m')
    
else:
    t1      = np.datetime64('2025-08-13 00:00:00')
    t2      = t1 + np.timedelta64(1,'D')
    
t           = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
nt          = len(t)
output      = np.ones((nt,4))*np.nan
cal_slope   = 2293 # For plotting somewhat realistic values, not used in output file.


for it in range(nt):
    tt      = t[it]

    if not tt in progress:
        O3        = Loobos_Read_NL_Loo_03(tt,tt+np.timedelta64(30,'m'),keys=None)
        ec        = Loobos_ReadGHG_Raw(   tt,tt+np.timedelta64(30,'m'),keys=None)

        timeIndex = pd.date_range(tt-pd.Timedelta('1m'),tt+pd.Timedelta('32m'),freq='50ms')
        O3        = O3[~O3.index.duplicated()]
        data      = pd.DataFrame(index=timeIndex,columns = ec.keys().append(O3.keys()))
        
        I         = O3.index.intersection(data.index);     data.loc[I,O3.keys()] = O3.loc[I,O3.keys()]
        I         = ec.index.intersection(data.index);     data.loc[I,ec.keys()] = ec.loc[I,ec.keys()]

        progress.append(tt)
    data['O3_uncal'] = (data['O3_raw']/np.power(10,data['multplier'])).astype(float)
    
    I         = ((data.index > tt) & (data.index < tt+pd.Timedelta('30m')) & (pd.isna(data['O3_uncal']) == False) )
    if len(np.where(I)[0]) > 1:
        idx       = data.index.values.astype(float)
        p         = np.polyfit(idx[I],data.loc[I,'O3_uncal'],1)
        data['O3_detrend'] = data['O3_uncal'] - (idx*p[0]+p[1]) + data['O3_uncal'].mean()
    else:
        data['O3_detrend'] = data['O3_uncal'] * np.nan
    
    if False:
        f1 = plt.figure(1,figsize=[10.2,6.6],dpi=96) # 976 x 637 
        f1.clf()
        ax = f1.add_subplot(211)
        ax.plot(data.index, cal_slope*data['O3_uncal'  ],'r',label='O3 uncal')
        ax.plot(data.index, cal_slope*(idx*p[0]+p[1])   ,'k-',label='Linear regression')
        ax.plot(data.index, cal_slope*data['O3_detrend'],'b',label='O3 detrended')
        ax.set_xlim([data.index[0],data.index[-1]])
        ax.set_ylabel('O3 uncal $-$ (*%d = ~ug/m3~)'%cal_slope,fontsize=FS)
        ax.set_title('O3 %s'%np.datetime_as_string(t1,unit='D'),fontsize=FS)
        dates_fmt = mdates.DateFormatter('%H:%M')
        ax.xaxis.set_major_formatter(dates_fmt)
        ax.grid(axis='both',color='grey',linestyle='--',alpha=0.5)
        ax.legend()
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label ] + ax.get_xticklabels() + ax.get_yticklabels() + ax.get_legend().get_texts()): item.set_fontsize(FS)
    
        ax = f1.add_subplot(212)
        ax.plot(data.index,data['W'],'b',label='w')
        ax.set_xlim([data.index[0],data.index[-1]])
        ax.set_xlabel('date-time (CEST=UTC+1)',fontsize=FS)
        ax.set_ylabel('W (m/s)',fontsize=FS)
        dates_fmt = mdates.DateFormatter('%H:%M')
        ax.xaxis.set_major_formatter(dates_fmt)
        ax.grid(axis='both',color='grey',linestyle='--',alpha=0.5)
        ax.legend()
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label ] + ax.get_xticklabels() + ax.get_yticklabels() + ax.get_legend().get_texts()): item.set_fontsize(FS)
        
    shifts  = np.arange(-80,81,1)
    nshifts = len(shifts)
    flux    = np.ones((nshifts,1))*np.nan
    for ishift in range(nshifts):
        shift = shifts[ishift]
        w     = data['W'].astype(float)
        C     = data['O3_detrend'].astype(float).shift(shift)

        I0    = ((w.index > tt) & (w.index < tt+pd.Timedelta('30m')) & (pd.isna(w)==False) & (pd.isna(C) == False))
        flux[ishift,0] = w.loc[I0].cov(C.loc[I0])
    i0    = np.where(  np.abs(flux[:,0]) == np.abs(flux[:,0]).max())
    #if len(i0) > 1: i0 = i0[0]                         #ESG_SB_20250815+ Fixed if function now correctly supports tuple to be reset to array([i],dtype=int64),)
    if len(i0[0]) > 1: i0 = (np.array([i0[0][0]]),)     #ESG_SB_20250815+ Fixed if function now correctly supports tuple to be reset to array([i],dtype=int64),)
    i24   = np.where(shifts == -24)
        
    I0    = ((data.index > tt) & (data.index < tt+pd.Timedelta('30m')) & (pd.isna(data['W'])==False) & (pd.isna(data['O3_raw']) == False) & (pd.isna(data['multplier']) == False))
    output[it,0] = data.loc[I0,'O3_detrend'].mean() # raw concentration
    if len(i0[0]) > 0:
        output[it,1] = flux[i0 ,0]    # V m/s
        output[it,2] = shifts[i0]/20 # s tau
    output[it,3] = flux[i24,0]   # s flux at fixed time delay

    if False:
        f2 = plt.figure(2)
        f2.clf()
        ax = f2.add_subplot(111)
        ax.plot(shifts/20, cal_slope*flux[:,0],color='g',label='O3 flux')
        ax.plot([shifts[i0]/20,shifts[i0]/20],ax.get_ylim(),'g--')
        
        ax.set_xlabel('shift (s)')
        ax.set_ylabel('O3 flux (ug/m2/s)')
        ax.legend()
        plt.savefig('Flux_O3_%s.png'%tt.astype(datetime.datetime).strftime('%Y%m%d%H%M'))

#--- Write daily output files.
output = pd.DataFrame(output,index=t)
output.index.name = 'TIMESTAMP'
output.columns = ['C_O3','F_O3','tau','F_O3_1s']
days = np.arange(t1,t2,np.timedelta64(1,'D'))
ndays = len(days)
for id in range(ndays):
    day = days[id]
    I = ((output.index >= day) & (output.index < day+np.timedelta64(1,'D')))
    filename = os.path.join(datapath,'NL-Loo_O3','%4d'%day.astype(object).year,'%02d'%day.astype(object).month,'NL-Loo_O3_%4d%02d%02d_L06_F62.csv'%(day.astype(object).year,day.astype(object).month,day.astype(object).day))
    output.loc[I].to_csv(filename,date_format = '%Y%m%d%H%M%S')

if False:
    f = plt.figure(3)
    f.clf()
    ax = f.add_subplot(3,1,1)
    ax.plot(output.index,cal_slope*output['C_O3'])
    ax.set_ylabel('C (ug/m3)')
    #ax.set_xlim(pd.Timestamp('2023-07-07'),pd.Timestamp('2023-07-10'))
    
    ax = f.add_subplot(3,1,2)
    ax.plot(output.index,cal_slope*output['F_O3'])
    ax.set_ylabel('F (ug/m2/s)')
    #ax.set_xlim(pd.Timestamp('2023-07-07'),pd.Timestamp('2023-07-10'))
    
    ax = f.add_subplot(3,1,3)
    ax.plot(output.index,output['tau'])
    ax.set_ylabel('tau (s)')
    #ax.set_xlim(pd.Timestamp('2023-07-07'),pd.Timestamp('2023-07-10'))    


    
# r = 0.002 # m 4 mm binnenleiding
# l = 5.5     # m
# V = np.pi * np.square(r) * l*1000 # m3 --> l/1000
# tau = 60*V/4.3
# print(tau) --> 0.96 s
