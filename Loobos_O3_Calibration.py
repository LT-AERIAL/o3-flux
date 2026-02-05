import numpy as np
import pandas as pd
import os
import sys
datapath_toolbox = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','zz_Python')
sys.path.insert(1, datapath_toolbox)
from Loobos_Toolbox_NewTower import *
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.cm as colormaps
from scipy.stats import linregress
#import matplotlib.dates as mdates

### Set days to process
d1                    = pd.to_datetime('2025-03-01')
d2                    = pd.to_datetime('2026-01-26') # inclusive
days                  = pd.date_range(d1,d2,freq='24h')

###--- Load data -------------------------------------------------------
if not 'dataloaded' in globals(): dataloaded = False
if not dataloaded:
    O3_data           = list()
    for day in days:
#       filename      = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','NL-Loo_O3','%4d'%day.year,'%02d'%day.month,'NL-Loo_O3_%4d%02d%02d_F65.csv'%(day.year,day.month,day.day))
        filename      = os.path.join('Y:\\','research-loobos-o3-voc-flux','NL-Loo_O3','%4d'%day.year,'%02d'%day.month,'NL-Loo_O3_%4d%02d%02d_F65.csv'%(day.year,day.month,day.day))
        O3_data      .append(pd.read_csv(filename,index_col='TIMESTAMP_START'))
    O3_data           = pd.concat(O3_data,axis=0, ignore_index=False)
    O3_data.index     = pd.to_datetime(O3_data.index)
    dataloaded        = True
#    O3_abs          : ug/m3               : Thermo 49C ozone concentration
#    T_bench         : deg C               : bench temperature      (must be between   5   and   50   deg C)
#    T_lamp          : deg C               : lamp  temperature      (must be between  50   and   60   deg C)
#    flow_A          : L/min               : flow rate in cell A    (must be between   0.4 and    1.6 L/min
#    flow_B          : L/min               : flow rate in cell B    (must be between   0.4 and    1.6 L/min
#    pressHg         : mm Hg               : air pressure in cell B (must be between 200   and 1000   mm Hg
#    press           : kPa                 : air pressure in cell B, converted to kPa   
#    O3_FOS_V        : Volt                : raw voltage output of O3 signal
#    FOS_flowrate    : L/min               : Flow rate in FOS (standard L/min) 
#    FOS_temp        : deg C               : temperature in FOS

O3_data['O3_FOS_V']   = O3_data['O3_FOS_V'].astype(float)
O3_data['O3_abs'  ]   = O3_data['O3_abs'  ].astype(float)
O3_data['SignalRatio']= 100*O3_data['O3_FOS_V']/ O3_data['O3_abs'  ] # Ratio of the FOS/Abs scaled to 0-0.25 (similar range as O3_FOS_V

###--- QA/QC
#Invalid data:        
I                     = ((O3_data.index >= pd.to_datetime('2025-09-07 00:00:00')) & (O3_data.index <= pd.to_datetime('2025-09-10 10:30:00')))
O3_data.loc[I,'QF']   = 1e6
I                     = (O3_data['QF'] > 0)
O3_data.loc[I,:]      = pd.NA

if True:
    ###--- Detect Disc changes
    t                     = O3_data.index
    O3_data['DiscChange'] = 0
    signal                = 'low'
    for tt in t:
        if   ((signal == 'low' ) & (O3_data.loc[tt,'O3_FOS_V'] > 0.025) & (O3_data.loc[tt,'SignalRatio'] > 0.07)):
            # Disc change detected
            signal        = 'high'
            O3_data.loc[tt,'DiscChange'] = 1
        elif ((signal == 'high') & (O3_data.loc[tt,'O3_FOS_V'] < 0.010) & (O3_data.loc[tt,'SignalRatio'] < 0.03)):
            signal        = 'low'

#                        O3_abs  T_bench  T_lamp  flow_A  flow_B  pressHg   press  O3_FOS_V  FOS_flowrate  FOS_temp   QF  SignalRatio  DiscChange  t_since
#  TIMESTAMP_START
#  2025-03-01 00:00:00    5.837    29.55    55.5  0.7160  0.7162   763.90  101.84  0.033115          4.39     19.97  0.0     0.567323           1        0
#  2025-03-17 10:50:00   44.004    28.48    55.5  0.6590  0.5842   909.38  121.24  0.033125          4.36     20.50  0.0     0.075277           1        0
#  2025-03-27 10:55:00   42.316    29.10    55.6  0.6542  0.5756   894.90  119.31  0.031079          4.27     23.19  0.0     0.073446           1        0
#  2025-04-05 16:40:00   74.400    28.40    55.5  0.6550  0.5690   895.02  119.33  0.145476           NaN       NaN  0.0     0.195533           1        0
#  2025-04-15 09:15:00   60.822    28.50    55.5  0.6458  0.5694   876.42  116.85  0.050867           NaN       NaN  0.0     0.083633           1        0
#  2025-04-19 17:05:00   76.056    28.80    55.6  0.6490  0.5730   884.84  117.97  0.108362           NaN       NaN  0.0     0.142477           1        0
#  2025-04-28 10:20:00   87.150    28.80    55.6  0.6578  0.5752   904.20  120.55  0.160074           NaN       NaN  0.0     0.183676           1        0
#  2025-05-06 14:55:00   66.594    28.10    55.5  0.6548  0.5718   896.06  119.46  0.078205          4.26     23.03  0.0     0.117436           1        0
#  2025-05-12 09:25:00   60.304    28.20    55.5  0.6524  0.5700   890.84  118.77  0.086129          4.10     30.69  0.0     0.142824           1        0
#  2025-05-20 09:25:00   68.670    28.30    55.5  0.6546  0.5706   896.04  119.46  0.097032          4.05     32.55  0.0     0.141301           1        0
#  2025-05-26 12:30:00   65.824    29.06    55.6  0.6546  0.5728   894.26  119.22  0.054248           NaN       NaN  0.0     0.082414           1        0
#  2025-06-04 09:55:00   52.772    28.80    55.6  0.6498  0.5586   882.36  117.64  0.039828          4.15     22.72  0.0     0.075473           1        0
#  2025-06-13 11:00:00  108.920    28.70    55.6  0.6998  0.5896   982.32  130.97  0.102783          3.95     42.62  0.0     0.094366           1        0
#  2025-06-19 10:15:00   60.988    28.30    55.5  0.7014  0.6070   991.60  132.20  0.075938           NaN       NaN  0.0     0.124514           1        0
#  2025-06-26 11:55:00   57.926    28.30    55.5  0.6926  0.6028   973.04  129.73  0.046306          4.05     30.16  0.0     0.079939           1        0
#  2025-07-02 08:40:00   71.084    28.20    55.5  0.6948  0.6000   974.90  129.98  0.055435           NaN       NaN  0.0     0.077985           1        0
#  2025-07-10 09:30:00   72.310    29.10    55.6  0.7006  0.6042   987.10  131.60  0.057193          4.11     30.27  0.0     0.079095           1        0
#  2025-07-18 09:40:00   60.750    29.12    55.6  0.6964  0.6000   978.10  130.40  0.057422           NaN       NaN  0.0     0.094522           1        0
#  2025-07-25 17:00:00   66.410    29.12    55.6  0.6444  0.5620   877.00  116.92  0.047108          4.21     31.05  0.0     0.070935           1        0
#  2025-08-13 09:50:00   88.316    28.90    55.6  0.6410  0.5598   872.00  116.26  0.080234           NaN       NaN  0.0     0.090848           1        0
#  2025-08-25 13:55:00   77.576    29.38    55.6  0.6270  0.6316   873.52  116.46  0.101973          4.05     35.57  0.0     0.131449           1        0
#  2025-09-10 10:20:00   24.274    29.54    55.6  0.6224  0.6276   862.00  114.92  0.052146          4.38     15.31  0.0     0.214821           1        0
#  2025-09-10 11:40:00   42.288    29.80    55.6  0.6218  0.6276   861.86  114.91  0.029690          4.33     24.14  0.0     0.070210           1        0
#  2025-09-13 10:35:00   43.486    29.60    55.6  0.6260  0.6314   869.10  115.87  0.058104           NaN       NaN  0.0     0.133616           1        0
#  2025-09-22 09:15:00   31.538    28.30    55.5  0.6198  0.6270   879.50  117.26  0.004155           NaN       NaN  0.0     0.013173           1        0
#  2025-10-16 11:00:00   58.750    28.40    55.5  0.6212  0.6272   881.10  117.47  0.033151           NaN       NaN  0.0     0.056428           1        0
#  2025-10-29 12:55:00   39.704    28.40    55.5  0.6116  0.6180   857.72  114.35  0.008028          2.99     19.93  0.0     0.020220           1        0
#  2025-11-19 16:50:00   47.666    28.86    55.5  0.6140  0.6200   859.58  114.60  0.035790          4.17     16.45  0.0     0.075085           1        0
#  2025-12-11 10:50:00   36.816    27.60    55.5  0.6212  0.6280   879.54  117.26  0.026482          3.99     20.47  0.0     0.071930           1        0
#  2025-12-23 13:10:00   29.942    29.18    55.6  0.6218  0.6270   877.14  116.94  0.026845          4.12     17.14  0.0     0.089658           1        0
#  2026-01-07 00:30:00   30.924    27.60    55.5  0.6268  0.6320   886.08  118.13  0.025334          4.15     19.99  0.0     0.081923           1        0
#  2026-01-21 14:00:00   24.064    28.58    55.5  0.6128  0.6202   859.18  114.55  0.025270          3.87     20.05  0.0     0.105010           1        0
    # Manual override for failing auto-detection:
    O3_data.loc[pd.Timestamp('2025-03-01 00:00:00'),'DiscChange'] = 0;
    O3_data.loc[pd.Timestamp('2025-03-17 10:50:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-03-17 10:20:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-03-27 10:55:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-03-27 10:45:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-04-05 16:40:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-04-15 09:15:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-04-19 17:05:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-04-28 10:20:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-05-06 14:55:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-05-12 09:25:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-05-12 09:20:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-05-20 09:25:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-05-26 12:30:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-06-04 09:55:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-06-04 09:30:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-06-13 11:00:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-06-13 10:55:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-06-19 10:15:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-06-26 11:55:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-06-26 11:40:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-07-02 08:40:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-07-10 09:30:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-07-10 09:25:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-07-18 09:40:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-07-25 17:00:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-07-25 09:25:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-08-13 09:50:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-08-13 09:45:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-08-25 13:55:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-08-25 13:50:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-09-10 10:20:00'),'DiscChange'] = 0; # remove
    O3_data.loc[pd.Timestamp('2025-09-10 11:40:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-09-10 10:35:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-09-13 10:35:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-09-22 09:15:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-10-16 11:00:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-10-29 12:55:00'),'DiscChange'] = 1;
    O3_data.loc[pd.Timestamp('2025-11-19 16:50:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-11-19 16:25:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-12-11 10:50:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-12-11 10:05:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-12-23 13:10:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-12-23 12:50:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2026-01-07 00:30:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2026-01-06 14:55:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2026-01-21 14:00:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2026-01-21 11:15:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-09-22 10:05:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-09-22 09:15:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-10-16 12:10:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-10-16 11:00:00'),'DiscChange'] = 1
    O3_data.loc[pd.Timestamp('2025-10-30 03:10:00'),'DiscChange'] = 0; O3_data.loc[pd.Timestamp('2025-10-29 12:55:00'),'DiscChange'] = 1
    
    # Calculate time since last disk change
    event                 = 1 - O3_data['DiscChange']
    event                 = O3_data['DiscChange'].lt(0.5)
    O3_data['t_since']    = event.cumsum()-event.cumsum().where(~event).ffill().fillna(0).astype(int) # units: 5 minute blocks

###--- Apply calibration over 8h blocks
if True:
    timeIndex         = pd.date_range(d1,d2+pd.Timedelta(1,'D'),freq='8h',inclusive='left')
    O3_cal            = pd.DataFrame(index=timeIndex,columns=['O3_abs_avg','O3_abs_std','O3_FOS_avg','O3_FOS_std','n','slope','intercept','rvalue','pvalue','stderr','intercept_stderr','ratio','CalFact'])
    O3_cal.index.name = 'TIMESTAMP_START'
    
    for tt in O3_cal.index:
        t1   = tt                        # day + pd.Timedelta( ib   *8,'h')
        t2   = tt + pd.Timedelta(8,'h')  # day + pd.Timedelta((ib+1)*8,'h')
        
        I    = ((O3_data.index >= t1) & (O3_data.index < t2))
        DiscChangeBlock = True if (O3_data.loc[I,'DiscChange'].sum() > 0) else False
        
        if not DiscChangeBlock:
            I   = ((O3_data.index >= t1) & (O3_data.index < t2) & (pd.isna(O3_data['O3_abs']) == False) & (pd.isna(O3_data['O3_FOS_V']) == False))
        else: # Use only time until DiscChange
            I   = ((O3_data.index    >= t1) & (O3_data.index < t2 ) & (pd.isna(O3_data['O3_abs']) == False) & (pd.isna(O3_data['O3_FOS_V']) == False))
            t22 = ((O3_data.index    >= t1) & (O3_data.index < t2 ) & (pd.isna(O3_data['O3_abs']) == False) & (pd.isna(O3_data['O3_FOS_V']) == False) & (O3_data['DiscChange'] == 1)).index
            I   = ((O3_data.index    >= t1) & (O3_data.index < t22))

        X                                        = O3_data.loc[I,'O3_abs'  ]
        Y                                        = O3_data.loc[I,'O3_FOS_V']
        it                                       = (O3_cal.index == t1)
        O3_cal    .loc[it,'O3_abs_avg'      ]    = X.mean()
        O3_cal    .loc[it,'O3_abs_std'      ]    = X.std()
        O3_cal    .loc[it,'O3_FOS_avg'      ]    = Y.mean()
        O3_cal    .loc[it,'O3_FOS_std'      ]    = Y.std()
        O3_cal    .loc[it,'n'               ]    = len(O3_data.loc[I])
        if len(O3_data.loc[I]) > 12*3:     # at least 3 hours available
            res = linregress(X,Y,nan_policy='omit')
            O3_cal.loc[it,'slope'           ]    = res.slope
            O3_cal.loc[it,'intercept'       ]    = res.intercept
            O3_cal.loc[it,'rvalue'          ]    = res.rvalue
            O3_cal.loc[it,'pvalue'          ]    = res.pvalue
            O3_cal.loc[it,'stderr'          ]    = res.stderr
            O3_cal.loc[it,'intercept_stderr']    = res.intercept_stderr
            O3_cal.loc[it,'ratio'           ]    = Y.mean()/X.mean()

    I = ( np.square((O3_cal['rvalue'])  > 0.8) & (O3_cal['pvalue'] <  0.001))
    O3_cal.loc[I,'CalFact']                      = O3_cal.loc[I,'slope']
    I = ( np.square((O3_cal['rvalue']) <= 0.8) | (O3_cal['pvalue'] >= 0.001))
    O3_cal.loc[I,'CalFact']                      = O3_cal.loc[I,'ratio']

###--- Make daily calibration files on half hourly basis
if True:
    datetimeIndex = pd.date_range(O3_cal.index[0],O3_cal.index[-1],freq='30min',inclusive='left')
#   O3_cal_hh = pd.DataFrame(columns=O3_cal.keys(),index=datetimeIndex)
    O3_cal_hh = O3_cal.reindex(datetimeIndex,method='ffill')
    O3_cal_hh.index.name = 'TIMESTAMP_START'

    formats                 = {'O3_abs':'{:7.3f}', 'T_bench':'{:5.2f}', 'T_lamp':'{:4.1f}', 'flow_A':'{:6.4f}', 'flow_B':'{:6.4f}', 'pressHg':'{:6.3f}', 'press':'{:6.2f}', 'O3_FOS_V':'{:12.10f}', 'FOS_flowrate':'{:6.2f}', 'FOS_temp':'{:5.2f}'}
    formats                 = {'O3_abs_avg'       : '{:6.2f}',
                              'O3_abs_std'       : '{:6.2f}',
                              'O3_FOS_avg'       : '{:7.5f}',
                              'O3_FOS_std'       : '{:7.5f}',
                              'n'                : '{:2d}',
                              'slope'            : '{:10.8f}',
                              'intercept'        : '{:10.8f}',
                              'rvalue'           : '{:7.3f}',
                              'pvalue'           : '{:7.3f}',
                              'stderr'           : '{:9.7f}',
                              'intercept_stderr' : '{:9.7f}', 
                              'ratio'            : '{:9.7f}',
                              'CalFact'          : '{:9.7f}'}
    for col, f in formats.items():
       O3_cal_hh[col] = O3_cal_hh[col].map(lambda x: f.format(x)).astype(float)

    for day in O3_cal_hh.index.map(pd.Timestamp.date).unique():
        print(day)
        I = (O3_cal_hh.index.date == day)
        outfilename = os.path.join('Y:\\','research-loobos-o3-voc-flux','NL-Loo_O3','%4d'%day.year,'%02d'%day.month,'NL-Loo_O3_%4d%02d%02d_F63.csv'%(day.year,day.month,day.day))
        O3_cal_hh.loc[I].to_csv(outfilename)


if True:
    f = plt.figure(1)
    f.clf()
    ax = f.add_subplot(111)
    ax.plot([O3_data.index[0],O3_data.index[-1]],[0.025, 0.025],'b-',linewidth=3)
    ax.plot([O3_data.index[0],O3_data.index[-1]],[0.010, 0.010],'b-',linewidth=3)
    ax.plot([O3_data.index[0],O3_data.index[-1]],[0.030, 0.030],'g-',linewidth=3)
    ax.plot([O3_data.index[0],O3_data.index[-1]],[0.070, 0.070],'g-',linewidth=3)
    
    I = (O3_data['DiscChange'] == 1)
    ax.plot([O3_data.index[I],O3_data.index[I]],[-0.01,0.2],'r-o',linewidth=2)
    ax.plot(O3_data['O3_FOS_V'],label='FOS')
    ax.plot(O3_data['O3_abs'  ]/2000,label='abs')
    
    I = ((O3_data['SignalRatio'] > -0.01) & (O3_data['SignalRatio']< 0.25))
    ax.plot(O3_data.loc[I,'SignalRatio'],label='ratio')
    ax.legend()

#    ax = f.add_subplot(212)
#
#    ax.set_ylabel('ratio')

#if True:
    f = plt.figure(2)
    f.clf()
    ax = f.add_subplot(111)
    ax.plot(   O3_cal['slope'],'g:x',label='slope')
    ax.plot(   O3_cal['ratio'],'b:+',label='ratio')
#   ax.plot(  (O3_cal['O3_FOS_avg']-O3_cal['intercept'])/O3_cal['O3_abs_avg'],'mo')
   #ax.plot(   O3_cal['ratio'],'k-o',label='best')
    ax.legend()
    
#    ax2 = ax.twinx()
#    ax2.plot(O3_cal['rvalue'],'r')
    
#if True:
    f = plt.figure(3)
    f.clf()
    ax = f.add_subplot(111)
    h=ax.scatter(O3_cal['slope'],O3_cal['ratio'],4,O3_cal['rvalue'],clim=[0.5, 1.0],cmap='RdYlGn')
    ax.plot([-0.001,0.003],[-0.001,0.003],'k-')
    ax.set_xlabel('slope')
    ax.set_ylabel('ratio')
    plt.colorbar(h)
    
#if True:
    f = plt.figure(4)
    f.clf()
    ax = f.add_subplot(111)
    plt.set_cmap('jet')

    Discs    = O3_data['DiscChange'].cumsum().resample('8h').min()
    nDiscs   = Discs.max()
    DiscTime = O3_data['t_since']            .resample('8h').min()/12
    cmap     = colormaps.jet
#   COLORS   = cmap(np.arange(nspecies)/nspecies)  
    COLORS   = cmap(np.arange(nDiscs)/nDiscs)  

    for iDisc in range(2,nDiscs+1):
        I = ((Discs == iDisc) & (O3_cal['slope'] > 5e-6))
        h=ax.plot(DiscTime.loc[I],O3_cal.loc[I,'slope'],'-' ,label=iDisc,color=COLORS[iDisc-1])
        I = ((Discs == iDisc) & (O3_cal['ratio'] > 5e-6))
        h=ax.plot(DiscTime.loc[I],O3_cal.loc[I,'ratio'],'--',label=None,color=COLORS[iDisc-1])
    
       #h=ax.scatter(O3_data.loc[I,['t_since'].resample('8h').min()/12,O3_cal['ratio'],3,O3_data['DiscChange'].cumsum().resample('8h').min(),'.',label='ratio')
       #ax.scatter(O3_data['t_since'].resample('8h').min()/12,O3_cal['slope'],3,O3_data['DiscChange'].cumsum().resample('8h').min(),'.',label='slope')
    
    ax.set_xlabel('disk time (h)')
    ax.set_ylabel('FOS Calibration factor ($\mu$g m$^{-3}$ V$^{-1})$')
    ax.set_yscale('log')
    ax.legend()

#if True:
    f = plt.figure(5)
    f.clf()
    ax = f.add_subplot(111)
#   plt.set_cmap('jet')

    Discs    = O3_data['DiscChange'].cumsum().resample('8h').min()
    nDiscs   = Discs.max()
    DiscTime = O3_data['t_since']            .resample('8h').min()/12
    cmap     = colormaps.jet
    Cmean    = O3_data['O3_abs'].resample('8h').mean()

    for iDisc in range(2,nDiscs+1):
        I = ((Discs == iDisc) & (O3_cal['ratio'] > 5e-6))
        h=ax.plot(DiscTime.loc[I],O3_cal.loc[I,'ratio'],'k--')
        h=ax.scatter(DiscTime.loc[I],O3_cal.loc[I,'ratio'],s=49,c=Cmean.loc[I],cmap=colormaps.jet,clim=[0,80])
    #norm = mpl.colors.Normalize(vmin=0, vmax=150)
    #scalar_mappable = mpl.cm.ScalarMappable(norm=norm, cmap=colormaps.jet)
    #f.colorbar(scalar_mappable, cax=ax, orientation='horizontal', label='O3 (ug/m3)')
    cb=plt.colorbar(h)
    cb.set_label('O3 (ug/m3)')
    ax.set_xlabel('disk time (h)')
    ax.set_ylabel('FOS Calibration factor ($\mu$g m$^{-3}$ V$^{-1}$)')
    ax.set_yscale('log')

#O3_cal.to_csv('O3_calibration.csv')