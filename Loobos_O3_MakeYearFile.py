import pandas as pd
import os
import matplotlib.pyplot as plt

datapath = os.path.join('Y:\\','research-loobos-o3-voc-flux','NL-Loo_O3')


t1 = pd.to_datetime('2025-03-01 00:00:00')
t2 = pd.to_datetime('2025-12-31 23:59:59')

days      = pd.date_range(t1,t2,freq='1D'   ,inclusive='left')
timeIndex = pd.date_range(t1,t2,freq='30min',inclusive='left')

if False:
    #O3_data = pd.DataFrame(index=timeIndex)
    O3_flux = list()
    O3_cal  = list()
    for day in days:
        infilename_F62 = os.path.join(datapath,'%4d'%day.year,'%02d'%day.month,'NL-Loo_O3_%4d%02d%02d_L06_F62.csv'%(day.year,day.month,day.day))
        infilename_F63 = os.path.join(datapath,'%4d'%day.year,'%02d'%day.month,'NL-Loo_O3_%4d%02d%02d_F63.csv'%(    day.year,day.month,day.day))
        O3_flux        .append(pd.read_csv(infilename_F62,index_col='TIMESTAMP'      , parse_dates=['TIMESTAMP',]      ,date_format='%Y%m%d%H%M%S'))
        O3_cal         .append(pd.read_csv(infilename_F63,index_col='TIMESTAMP_START', parse_dates=['TIMESTAMP_START',]                           ))
    
    O3_flux = pd.concat(O3_flux,axis=0)
    O3_cal  = pd.concat(O3_cal ,axis=0)
        
    O3_flux.index.name = 'TIMESTAMP_START'
    #O3_data = pd.concat([O3_data,O3_flux],axis=0)
        #O3_data = pd.concat([O3_data,O3_cal ],axis=0)
    O3_data = pd.concat([O3_cal,O3_flux.loc[:,['F_O3','tau','F_O3_1s']]],axis=1)
    
    year = 2025
    outfilename = os.path.join(datapath,'%4d'%year,'NL-Loo_O3_Cal_Flux_%4d.csv'%year)
    O3_data.to_csv(outfilename)

if True:
    
    f = plt.figure(1)
    f.clf()
    ax = f.add_subplot(211)
    ax.plot(100*O3_data['F_O3']/O3_data['ratio']/O3_data['O3_abs_avg'],'.')
    #                    V m/s / (V/(ug/m3)) 
    
    ax = f.add_subplot(212)
    ax.plot(O3_data['ratio'],'r.')
    ax.plot(O3_data['CalFact'],'b.')
    