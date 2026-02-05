import numpy as np
import pandas as pd
import zipfile
import os
from datetime import date
from sys import platform

def getDatapath(datapath):
    if datapath is None:
        if platform == 'win32':
            if os.path.exists(os.path.join('C:\\','Data')):
                datapath         = os.path.join('C:\\','Data') # Loobos PC
            else:
                datapath         = os.path.join('w:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive')
        else:
            datapath             = os.path.join('/Volumes','MAQ_Archive','loobos_archive')
    return datapath

def dateparse_bm1(t): # yyyy-mm-dd hh:mm:ss
   #print('In dateparser_bm1',t)
    t_index_bm          = pd.to_datetime(t,format='%Y-%m-%d %H:%M:%S')
    return t_index_bm

def dateparse_bm2(t): # yyyymmddhhmmss.ss
    t_index_bm          = pd.to_datetime(t,format='%Y%m%d%H%M%S.%f')
    return t_index_bm

def dateparse_bm3(t): # yyyymmddhhmmss 
    t_index_bm          = pd.to_datetime(t,format='%Y%m%d%H%M%S')
    return t_index_bm

def dateparse_aiudata   (date,time):
    t_index_ec          = pd.to_datetime(date + ' ' + time,format='%Y-%m-%d %H:%M:%S:%f')
    return t_index_ec

def dateparse_ec_fluxnet(TIMESTAMP_END):
    t_index_ec          = pd.to_datetime(TIMESTAMP_END,format='%Y%m%d%H%M')
    return t_index_ec

def dateparse_ec_full   (date,time):
    print(' in dateparse_ec_full:',date,time)
    t_string = date + ' ' + time
    t_index_ec          = pd.to_datetime(t_string,format='%Y-%m-%d %H:%M')
    return t_index_ec
    
def dateparse_aq        (date,time):
    t_index_aq          = pd.to_datetime(date + ' ' + time,format='%d-%m-%Y %H:%M:%S.%f')
    return t_index_aq    

###--- Note GHG files:
# tot 2022-05-31 10:30 is de tijd in de filename in CEST in plaats van CET. We weten nog niet of het in de ICOS files wel goed is.

###--- Loobos_Read_GHG_EddyPro_full ---------------------------------------------------------------------------------
def Loobos_Read_GHG_EddyPro_full   (t1,t2,keys=None,get_keys=False,datapath=None):
    #from Loobos_Toolbox_NewTower import dateparse_ec_full, getDatapath
    # https://api.dataplatform.knmi.nl/open-data/v1/datasets/radar_tar_refl_composites/versions/1.0/files

    all_keys = ['filename','date','time','DOY','daytime','file_records','used_records','Tau','qc_Tau','rand_err_Tau','H','qc_H','rand_err_H','LE','qc_LE','rand_err_LE','co2_flux','qc_co2_flux','rand_err_co2_flux','h2o_flux','qc_h2o_flux','rand_err_h2o_flux','ch4_flux','qc_ch4_flux','rand_err_ch4_flux','none_flux','qc_none_flux','rand_err_none_flux','H_strg','LE_strg','co2_strg','h2o_strg','ch4_strg','none_strg','co2_v-adv','h2o_v-adv','ch4_v-adv','none_v-adv','co2_molar_density','co2_mole_fraction','co2_mixing_ratio','co2_time_lag','co2_def_timelag','h2o_molar_density','h2o_mole_fraction','h2o_mixing_ratio','h2o_time_lag','h2o_def_timelag','ch4_molar_density','ch4_mole_fraction','ch4_mixing_ratio','ch4_time_lag','ch4_def_timelag','none_molar_density','none_mole_fraction','none_mixing_ratio','none_time_lag','none_def_timelag','sonic_temperature','air_temperature','air_pressure','air_density','air_heat_capacity','air_molar_volume','ET','water_vapor_density','e','es','specific_humidity','RH','VPD','Tdew','u_unrot','v_unrot','w_unrot','u_rot','v_rot','w_rot','wind_speed','max_wind_speed','wind_dir','yaw','pitch','roll','u*','TKE','L','(z-d)/L','bowen_ratio','T*','model','x_peak','x_offset','x_10%','x_30%','x_50%','x_70%','x_90%','un_Tau','Tau_scf','un_H','H_scf','un_LE','LE_scf','un_co2_flux','co2_scf','un_h2o_flux','h2o_scf','un_ch4_flux','ch4_scf','un_none_flux','un_none_scf','spikes_hf','amplitude_resolution_hf','drop_out_hf','absolute_limits_hf','skewness_kurtosis_hf','skewness_kurtosis_sf','discontinuities_hf','discontinuities_sf','timelag_hf','timelag_sf','attack_angle_hf','non_steady_wind_hf','u_spikes','v_spikes','w_spikes','ts_spikes','co2_spikes','h2o_spikes','ch4_spikes','none_spikes','head_detect_LI-7200','t_out_LI-7200','t_in_LI-7200','aux_in_LI-7200','delta_p_LI-7200','chopper_LI-7200','detector_LI-7200','pll_LI-7200','sync_LI-7200','chopper_LI-7500','detector_LI-7500','pll_LI-7500','sync_LI-7500','not_ready_LI-7700','no_signal_LI-7700','re_unlocked_LI-7700','bad_temp_LI-7700','laser_temp_unregulated_LI-7700','block_temp_unregulated_LI-7700','motor_spinning_LI-7700','pump_on_LI-7700','top_heater_on_LI-7700','bottom_heater_on_LI-7700','calibrating_LI-7700','motor_failure_LI-7700','bad_aux_tc1_LI-7700','bad_aux_tc2_LI-7700','bad_aux_tc3_LI-7700','box_connected_LI-7700','mean_value_RSSI_LI-7200','mean_value_LI-7500','u_var','v_var','w_var','ts_var','co2_var','h2o_var','ch4_var','none_var','w/ts_cov','w/co2_cov','w/h2o_cov','w/ch4_cov','w/none_cov','co2_mean','h2o_mean','co2_mean','h2o_mean','flowrate_mean','hit_power_mean','hit_vin_mean']
    if keys is None:
        keys = all_keys
    if get_keys == True:
        return all_keys
    datapath = getDatapath(datapath)
    t                    = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    nt                   = len(t)
    timeIndex            = pd.DatetimeIndex(t)
    ec                   = [] 
    for it in range(nt):
        tt               = t[it]-np.timedelta64(30,'m') # the 00:30 data are in the yyyy-mm-ddT000000_AIU-2066.ghg file
        yy               = tt.astype(object).year
        mm               = tt.astype(object).month
        dd               = tt.astype(object).day
        HH               = tt.astype(object).hour
        MM               = tt.astype(object).minute
        date             = np.datetime64('%4d-%02d-%02d'%(yy,mm,dd))
        if date < np.datetime64('2021-09-14'):
            ghgfilename      = os.path.join(datapath,'NL-Loo_EC','%04d'%yy,'%02d'%mm, '%4d-%02d-%02dT%02d%02d00_AIU-2066.zip' %(yy,mm,dd,HH,MM))
        else:
            ghgfilename      = os.path.join(datapath,'NL-Loo_EC','%04d'%yy,'%02d'%mm, '%4d-%02d-%02dT%02d%02d00_AIU-2066.ghg' %(yy,mm,dd,HH,MM))
        if os.path.exists(ghgfilename):
            #ESG_SB_20230915+ Changed the following section to do an additional check whether Python recognized the ghgfile as a .zip file. Previously this would cause a crash of the script.
            if zipfile.is_zipfile(ghgfilename):
                ghgfile      = zipfile.ZipFile(ghgfilename, 'r')
                file_success = True
            else:
                file_success = False
        else:
            file_success = False
    
        if file_success:
            files        = ghgfile.namelist()
            ecfilename   = [file for file in files if 'full_output' in file]
           
            if ecfilename:
                ecfilename = ecfilename[0]
                print('Found file v1: %s'%ecfilename)
                ec              .append(pd.read_csv(ghgfile.open(ecfilename),sep=',',header=1,skiprows=[2,],date_format='%Y-%m-%d %H:%M',parse_dates=[['date','time']],index_col='date_time'))
    
    ec          = pd.concat(ec, axis=0,ignore_index=False)
    ec          = ec.reindex(timeIndex)#.ffill()
    return ec

###--- Loobos_Read_GHG_EddyPro_full ---------------------------------------------------------------------------------
def Loobos_Read_GHG_EddyPro_fluxnet(t1,t2,keys=None,get_keys=False,datapath=None):
    #from Loobos_Toolbox_NewTower import dateparse_ec_fluxnet, getDatapath
    # https://api.dataplatform.knmi.nl/open-data/v1/datasets/radar_tar_refl_composites/versions/1.0/files

    all_keys = ['filename','date','time','DOY','daytime','file_records','used_records','Tau','qc_Tau','rand_err_Tau','H','qc_H','rand_err_H','LE','qc_LE','rand_err_LE','co2_flux','qc_co2_flux','rand_err_co2_flux','h2o_flux','qc_h2o_flux','rand_err_h2o_flux','ch4_flux','qc_ch4_flux','rand_err_ch4_flux','none_flux','qc_none_flux','rand_err_none_flux','H_strg','LE_strg','co2_strg','h2o_strg','ch4_strg','none_strg','co2_v-adv','h2o_v-adv','ch4_v-adv','none_v-adv','co2_molar_density','co2_mole_fraction','co2_mixing_ratio','co2_time_lag','co2_def_timelag','h2o_molar_density','h2o_mole_fraction','h2o_mixing_ratio','h2o_time_lag','h2o_def_timelag','ch4_molar_density','ch4_mole_fraction','ch4_mixing_ratio','ch4_time_lag','ch4_def_timelag','none_molar_density','none_mole_fraction','none_mixing_ratio','none_time_lag','none_def_timelag','sonic_temperature','air_temperature','air_pressure','air_density','air_heat_capacity','air_molar_volume','ET','water_vapor_density','e','es','specific_humidity','RH','VPD','Tdew','u_unrot','v_unrot','w_unrot','u_rot','v_rot','w_rot','wind_speed','max_wind_speed','wind_dir','yaw','pitch','roll','u*','TKE','L','(z-d)/L','bowen_ratio','T*','model','x_peak','x_offset','x_10%','x_30%','x_50%','x_70%','x_90%','un_Tau','Tau_scf','un_H','H_scf','un_LE','LE_scf','un_co2_flux','co2_scf','un_h2o_flux','h2o_scf','un_ch4_flux','ch4_scf','un_none_flux','un_none_scf','spikes_hf','amplitude_resolution_hf','drop_out_hf','absolute_limits_hf','skewness_kurtosis_hf','skewness_kurtosis_sf','discontinuities_hf','discontinuities_sf','timelag_hf','timelag_sf','attack_angle_hf','non_steady_wind_hf','u_spikes','v_spikes','w_spikes','ts_spikes','co2_spikes','h2o_spikes','ch4_spikes','none_spikes','head_detect_LI-7200','t_out_LI-7200','t_in_LI-7200','aux_in_LI-7200','delta_p_LI-7200','chopper_LI-7200','detector_LI-7200','pll_LI-7200','sync_LI-7200','chopper_LI-7500','detector_LI-7500','pll_LI-7500','sync_LI-7500','not_ready_LI-7700','no_signal_LI-7700','re_unlocked_LI-7700','bad_temp_LI-7700','laser_temp_unregulated_LI-7700','block_temp_unregulated_LI-7700','motor_spinning_LI-7700','pump_on_LI-7700','top_heater_on_LI-7700','bottom_heater_on_LI-7700','calibrating_LI-7700','motor_failure_LI-7700','bad_aux_tc1_LI-7700','bad_aux_tc2_LI-7700','bad_aux_tc3_LI-7700','box_connected_LI-7700','mean_value_RSSI_LI-7200','mean_value_LI-7500','u_var','v_var','w_var','ts_var','co2_var','h2o_var','ch4_var','none_var','w/ts_cov','w/co2_cov','w/h2o_cov','w/ch4_cov','w/none_cov','co2_mean','h2o_mean','co2_mean','h2o_mean','flowrate_mean','hit_power_mean','hit_vin_mean']
    if keys is None:
        keys = all_keys
    if get_keys == True:
        return all_keys
    datapath             = getDatapath(datapath)
    t                    = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    nt                   = len(t)
    timeIndex            = pd.DatetimeIndex(t)
    ec                   = [] 
    for it in range(nt):
        tt               = t[it]
        yy               = tt.astype(object).year
        mm               = tt.astype(object).month
        dd               = tt.astype(object).day
        HH               = tt.astype(object).hour
        MM               = tt.astype(object).minute
        ghgfilename      = os.path.join(datapath,'NL-Loo_EC','%4d'%yy,'%02d'%mm, '%4d-%02d-%02dT%02d%02d00_AIU-2066.ghg' %(yy,mm,dd,HH,MM))
        if os.path.exists(ghgfilename):
            ghgfile      = zipfile.ZipFile(ghgfilename, 'r')
            file_success = True
        else:
            file_success = False
    
        if file_success:
            files        = ghgfile.namelist()
            ecfilename   = [file for file in files if 'fluxnet' in file][0]
            if ecfilename:
                ec              .append(pd.read_csv(ghgfile.open(ecfilename),sep=',',header=0,date_format='%Y%m%d%H%M',usecols=keys,index_col='TIMESTAMP_END'))
    if len(ec) > 0:
        ec          = pd.concat(ec, axis=0,ignore_index=False)
        ec          = ec.reindex(timeIndex)#.ffill()
    else:
        ec = pd.DataFrame(index=timeIndex,columns=keys)

    return ec

###--- AIU files ----------------------------------------------------------------------------------------------------    
def Loobos_ReadGHG_Raw             (t1,t2,keys=None, get_keys=False,datapath=None):
    from Loobos_Toolbox_NewTower import dateparse_aiudata, getDatapath #,dateparse_ecraw

    all_keys             = ['TIMESTAMP','U','V','W','T_SONIC','CO2_CONC','H2O_CONC','SA_DIAG_TYPE','SA_DIAG_VALUE','GA_DIAG_CODE','T_CELL_IN','T_ABS']
    if keys is None:
        keys = all_keys
    if get_keys == True:
        return all_keys
    datapath             = getDatapath(datapath)
    t                    = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    nt                   = len(t)
    timeIndex            = pd.DatetimeIndex(t)
    ec                   = [] 
    for it in range(nt):
        tt               = t[it]
        yy               = tt.astype(object).year
        mm               = tt.astype(object).month
        dd               = tt.astype(object).day
        HH               = tt.astype(object).hour
        MM               = tt.astype(object).minute
        try:
            if tt <= np.datetime64('2021-12-08 11:00'):
                filename     = 'NL-Loo_EC_%4s%02d%02d%02d%02d_L1_F2'%(yy,mm,dd,HH,MM)
            else:
                filename     = 'NL-Loo_EC_%4s%02d%02d%02d%02d_L01_F02'%(yy,mm,dd,HH,MM)
            rawzipfilename   = os.path.join(datapath, 'NL-Loo_EC','%4d'%yy,'%02d'%mm,filename +'.zip' )
            rawzipfile       = zipfile.ZipFile(rawzipfilename, 'r')
            print('Found file v1: %s'%rawzipfilename)
            ec               .append(pd.read_csv(rawzipfile.open(filename +'.csv' ),sep=',',header=0,date_format='',             usecols=keys,index_col='TIMESTAMP')) ### not used??
            file_success     = 'v1'
            
        except FileNotFoundError as error:
            filename         = '%4d-%02d-%02dT%02d%02d00_AIU-2066'%(yy,mm,dd,HH,MM)
            rawzipfilename   = os.path.join(datapath, 'NL-Loo_EC','%4d'%yy,'%02d'%mm,filename+'.ghg' )
            if os.path.exists(rawzipfilename):
                print('Found file v2: %s'%rawzipfilename)
                try:
                    rawzipfile       = zipfile.ZipFile(rawzipfilename, 'r')
                    new_keys         = ['Date','Time','U (m/s)','V (m/s)','W (m/s)','T (C)','CO2 (mmol/m^3)','Temperature In (C)','H2O (mmol/m^3)','Anemometer Diagnostics']
                   #ec_tmp           = pd.read_csv(rawzipfile.open(filename +'.data'),sep='\t',header=7,date_format='%Y-%m-%d %H:%M:%S:%f',parse_dates=[['Date','Time']],usecols=new_keys,index_col='Date_Time')
                    ec_tmp           = pd.read_csv(rawzipfile.open(filename +'.data'),sep='\t',header=7,usecols=new_keys)
                    ec_tmp.index     = pd.to_datetime(ec_tmp['Date'] + ' ' + ec_tmp['Time'],format='%Y-%m-%d %H:%M:%S:%f')
                    ec_tmp.index.name = 'TIMESTAMP_START'
                    ec_tmp           = ec_tmp.rename({'U (m/s)': 'U', 'V (m/s)': 'V','W (m/s)': 'W', 'T (C)': 'T_SONIC','CO2 (mmol/m^3)': 'CO2_CONC', 'H2O (mmol/m^3)': 'H2O_CONC','Temperature In (C)':'T_CELL_IN'}, axis=1)  # new method
                    ec_tmp['SA_DIAG_TYPE' ] = np.nan
                    ec_tmp['SA_DIAG_VALUE'] = np.nan
                    ec               .append(ec_tmp)
                    file_success     = 'v2'
                except:# BadZipFile as error:
                    print('File is not a zipfile: RawEC %04d-%02d-%02d %02d:%02d'%(yy,mm,dd,HH,MM))
            else:
                print('File not found: RawEC %04d-%02d-%02d %02d:%02d'%(yy,mm,dd,HH,MM))
                file_success     = False
             
    
    if len(ec) > 0:        
        ec          = pd.concat(ec, axis=0,ignore_index=False)
        I = (ec['T_SONIC'] > 200)            
        ec.loc[I,'T_SONIC'] = ec.loc[I,'T_SONIC'] - 273.15
        if not 'T_CELL_IN' in ec.keys():
            ec['T_CELL_IN'] = np.nan
    else:
        ec  = pd.DataFrame(index=timeIndex, columns=keys)
#   ec      = ec.loc[:,keys]

    return ec    

###--- (NL-Loo_ST_yyyymmdd_L02_F11.csv) BM Meteo --------------------------------------------------------------------
def Loobos_Read_NL_Loo_BM          (t1,t2,keys=None,get_keys=False,datapath=None, API=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath

    datapath             = getDatapath(datapath)
    all_keys             = ['TIMESTAMP',   'SW_IN_1_1_1', 'SW_OUT_1_1_1', 'LW_IN_1_1_1', 'LW_OUT_1_1_1', 'LW_T_BODY_1_1_1', 'PPFD_IN_1_1_1', 'PPFD_OUT_1_1_1', 'TA_1_1_1', 'RH_1_1_1', 'TA_2_1_1', 'TA_2_2_1', 'TA_2_3_1', 'TA_2_4_1', 'TA_2_5_1', 'WS_2_1_1', 'WS_2_2_1', 'WS_2_3_1', 'WS_2_4_1','WS_2_5_1', 'WD_2_1_1', 'WD_2_2_1', 'WD_2_3_1', 'WD_2_4_1','WD_2_5_1','PA_1_1_1']
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
    if API == True:      # For the API we want one entire day file. The dayfiles start 1 minute after midnight and run until midnight.
        t_shift          = np.timedelta64(1,'m')
    else:
        t_shift          = 0
 
    t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(20,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm                   = [] 
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L02_F11.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM', 'raw','NL-Loo_BM_L02-Meteo.dat')
        # Try filename
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:          # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            
        if file_success:
            if (day >= date(2022, 8,31)) & (day <= date(2022, 9,1)):
                timestamp_str = 'Time_Stamp'
            else: 
                timestamp_str = 'TIMESTAMP'
            keys[0] = timestamp_str
            
            converters = {key: float for key in all_keys if not key == timestamp_str}
            if   (filename == filename_tmp):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-06-06')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-06-26')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-06-28')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-06-30')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-07-01')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-07-03')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-07-04')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-07-20')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-07-22')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-07-26')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-07-27')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y-%m-%d %H:%M:%S'))
            elif (day <= np.datetime64('2022-08-30')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            elif (day <= np.datetime64('2022-08-31')):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S')) 
            elif day <= np.datetime64('2022-11-06'):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S.%f'))
            else:
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],index_col=timestamp_str,converters=converters,usecols=keys,date_format='%Y%m%d%H%M%S')) 
    if len(bm) > 0:                
        bm          = pd.concat(bm, axis=0,ignore_index=False)
        bm          = bm[~bm.index.duplicated()]
        bm          = bm.reindex(timeIndex)#.ffill()
    else:
        bm          = pd.DataFrame(index=timeIndex, columns=keys)

    return bm

###--- (NL-Loo_BM_yyyymmdd_L04_F41.csv) BM Soil ---------------------------------------------------------------------
def Loobos_Read_NL_Loo_BM_Soil     (t1,t2,keys=None,get_keys=False, datapath=None, API=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath 

    datapath             = getDatapath(datapath)
    if t1 < np.datetime64('2023-01-12'):
        dt               = 20
    else:
        dt               = 60
    if API == True:      # For the API we want one entire day file. The dayfiles start 1 minute after midnight and run until midnight.
        t_shift          = np.timedelta64(1,'m')
    else:
        t_shift          = 0
    t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(dt,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm_soil              = [] 
    for iday in range(ndays):

        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM-Soil','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L04_F41.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM-Soil','raw','NL-Loo_BM-L03-Soil.dat')
        
        all_keys         = ["TIMESTAMP","TS_1_1_1","TS_1_2_1","TS_1_2_2","TS_1_3_1","TS_1_4_1","TS_1_5_1","TS_1_6_1","T_WTD_1_7_1","TS_2_1_1","TS_2_2_1","TS_2_2_2","TS_2_3_1","TS_2_4_1","TS_2_5_1","TS_2_6_1","T_WTD_2_7_1","TS_3_1_1","TS_3_2_1","TS_3_2_2","TS_4_1_1","TS_4_2_1","TS_4_2_2","SWC_1_1_1","SWC_1_2_1","SWC_1_3_1","SWC_1_4_1","SWC_1_5_1","SWC_2_1_1","SWC_2_2_1","SWC_2_3_1","SWC_2_4_1","SWC_2_5_1","SWC_3_1_1","SWC_4_1_1","G_1_1_1","G_ISCAL_1_1_1","G_2_1_1","G_ISCAL_2_1_1","G_3_1_1","G_ISCAL_3_1_1","G_4_1_1","G_ISCAL_4_1_1","WTD_1_1_1","WTD_2_1_1","SWC_IU_1_1_1","SWC_IU_1_2_1","SWC_IU_1_3_1","SWC_IU_1_4_1","SWC_IU_1_5_1","SWC_IU_2_1_1","SWC_IU_2_2_1","SWC_IU_2_3_1","SWC_IU_2_4_1","SWC_IU_2_5_1","SWC_IU_3_1_1","SWC_IU_4_1_1","G_IU_1_1_1","G_IU_2_1_1","G_IU_3_1_1","G_IU_4_1_1","G_SF_1_1_1","G_SF_2_1_1","G_SF_3_1_1","G_SF_4_1_1","WCP_1_1_1","WCP_2_1_1"]
        if keys is None:
           keys          = all_keys
        if get_keys == True:
            return all_keys

        if os.path.exists(filename):               # Try filename
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:                                      # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            
        if file_success:
            converters = {key: float for key in all_keys if not key == 'TIMESTAMP'}
            if filename == filename_tmp: 
                bm_soil.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',converters=converters,usecols=keys,encoding = "ISO-8859-1",date_format='%Y-%m-%d %H:%M:%S'))
            else:
                bm_soil.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',converters=converters,usecols=keys,encoding = "ISO-8859-1",date_format='%Y%m%d%H%M%S'))
    if len(bm_soil) > 0:
        bm_soil          = pd.concat(bm_soil, axis=0,ignore_index=False)
        bm_soil          = bm_soil[~bm_soil.index.duplicated()]
        bm_soil          = bm_soil.reindex(timeIndex)#.ffill()
    else:
        bm_soil          = pd.DataFrame(index=timeIndex, columns=keys)
    
    # --- Water Table Depth
    #wtd_1_1_1          = -8.88 + 1000*bm_soil['Press']      / (rho_H2O * g) # kPa --> Pa --> m # West  plot. Gemeten 06 Feb 2023 12:24h. WCP = 31.46 kPa --> 3.18 m. Grondwaterstand was -5.70 m tov maaiveld. Dus de sensor was op -8.88 m
    #wtd_2_1_1          = -9.59 + 1000*bm_soil['CS451_2(1)'] / (rho_H2O * g) # kPa --> Pa --> m # North plot. Gemeten 15 Feb 2023 11:18h. WCP = 31.40 kPa --> 3.17 m. Grondwaterstand was -6.42 m tov maaiveld. Dus de sensor was op -9.59 m
    
     
    #ESG_SB_20231030+ Changed removing duplicates based on column name instead of column contents
    #bm_soil = bm_soil.T.drop_duplicates().T
    bm_soil = bm_soil.loc[:,~bm_soil.columns.duplicated()].copy()
    #ESG_SB_20231030-
    
    if t2 <= np.datetime64('2023-01-12'):
        bm_soil = bm_soil.resample('1min').mean()
    
    return bm_soil
    
###--- (NL-Loo_BM_yyyymmdd_L04_F41.csv) BM MM ---------------------------------------------------------------------
def Loobos_Read_NL_Loo_BM_MM     (t1,t2,keys=None,get_keys=False, datapath=None, API=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath 

    datapath             = getDatapath(datapath)
    dt                    = 3600
    t_shift          = 0
    if API == True:      # For the API we want one entire day file. The dayfiles start 1 minute after midnight and run until midnight.
        t_shift          = np.timedelta64(1,'h')
    t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(dt,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm_mm              = []
    for iday in range(ndays):

        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM-MM','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L11_F01.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM-MM','raw','NL-Loo_BM_L11-MM.dat')
                
        all_keys         = ["TIMESTAMP","BattV_Min","PTemp_C_Avg","throughfall_1_tips_Tot","throughfall_1_mm_Tot","dendro_volt_Avg","dendro_um_Avg","Tensio_Temp_1_Avg","Tensio_Pres_1_Avg","Tensio_Tens_1_Avg","Tensio_VWC_Raw_1_Avg","Tensio_VWC_1_Avg","Tensio_Temp_2_Avg","Tensio_Pres_2_Avg","Tensio_Tens_2_Avg","Tensio_VWC_Raw_2_Avg","Tensio_VWC_2_Avg","Tensio_Temp_3_Avg","Tensio_Pres_3_Avg","Tensio_Tens_3_Avg","Tensio_VWC_Raw_3_Avg","Tensio_VWC_3_Avg","Tensio_Temp_4_Avg","Tensio_Pres_4_Avg","Tensio_Tens_4_Avg","Tensio_VWC_Raw_4_Avg","Tensio_VWC_4_Avg","Tensio_Temp_5_Avg","Tensio_Pres_5_Avg","Tensio_Tens_5_Avg","Tensio_VWC_Raw_5_Avg","Tensio_VWC_5_Avg","Tensio_Temp_6_Avg","Tensio_Pres_6_Avg","Tensio_Tens_6_Avg","Tensio_VWC_Raw_6_Avg","Tensio_VWC_6_Avg","Ka_1_Avg","SoilTemp_1_Avg","VWC_1_Avg","Ka_2_Avg","SoilTemp_2_Avg","VWC_2_Avg","Ka_3_Avg","SoilTemp_3_Avg","VWC_3_Avg","Ka_4_Avg","SoilTemp_4_Avg","VWC_4_Avg","Ka_5_Avg","SoilTemp_5_Avg","VWC_5_Avg","Ka_6_Avg","SoilTemp_6_Avg","VWC_6_Avg"]
        if keys is None:
           keys          = all_keys
        if get_keys == True:
            return all_keys

        if os.path.exists(filename):               # Try filename
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:                                      # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
                        
        if file_success:
            converters = {key: float for key in all_keys if not key == 'TIMESTAMP'}
            if filename == filename_tmp: 
                bm_mm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',converters=converters,usecols=keys,encoding = "ISO-8859-1",date_format='%Y-%m-%d %H:%M:%S'))
            else:
                bm_mm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',converters=converters,usecols=keys,encoding = "ISO-8859-1",date_format='%Y%m%d%H%M%S'))

    if len(bm_mm) > 0:
        bm_mm          = pd.concat(bm_mm, axis=0,ignore_index=False)
        bm_mm          = bm_mm[~bm_mm.index.duplicated()]
        bm_mm          = bm_mm.reindex(timeIndex)#.ffill()
    else:
        bm_mm          = pd.DataFrame(index=timeIndex, columns=keys)
    
    #ESG_SB_20231030+ Changed removing duplicates based on column name instead of column contents
    #bm_mm = bm_mm.T.drop_duplicates().T
    bm_mm = bm_mm.loc[:,~bm_mm.columns.duplicated()].copy()
    #ESG_SB_20231030-
        
    return bm_mm


###--- (NL-Loo_BM_Precipitation_yyyymmdd_L04_F45.csv) BM Precipitation --------------------------------------------------------------------
def Loobos_Read_NL_Loo_BM_Precip   (t1,t2,keys=None,get_keys=False,datapath=None, API=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath

    datapath             = getDatapath(datapath)
    timestamp_str   = 'TIMESTAMP'
    all_keys             = ["TIMESTAMP","P_1_1_1"]
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
        
    if API == True:      # For the API we want one entire day file. The dayfiles start 1 minute after midnight and run until midnight.
        t_shift          = np.timedelta64(1,'m')
    else:
        t_shift          = 0
    t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(60,'s')).astype(np.datetime64)
#   t                    = np.arange(t1,        t2,        np.timedelta64(60,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm                   = [] 
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM-Precipitation','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L04_F45.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM-Precipitation', 'raw','NL-Loo_BM_L04-Soil_precipitation.dat')
        # Try filename
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:          # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            

        if file_success:
            converters = {key: float for key in all_keys if not key == timestamp_str}
            if   (filename == filename_tmp):
                bm.append(pd.read_csv(os.path.join(datapath,filename_tmp),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y-%m-%d %H:%M:%S',index_col=timestamp_str,converters=converters,usecols=keys))
            else:
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S',index_col=timestamp_str,converters=converters,usecols=keys))
    if len(bm) > 0:                
        bm          = pd.concat(bm, axis=0,ignore_index=False)    
        bm          = bm[~bm.index.duplicated()]
        try:  bm = bm.reindex(timeIndex,method='bfill')     #ESG_SB_20231201+ bfill needed for 'raw' (today) data while it won't work for historical data.
        except: bm = bm.reindex(timeIndex)#.ffill()
    else:
        bm = pd.DataFrame(index=timeIndex, columns=keys)
           
    return bm

###--- (NL-Loo_BM_Backup_yyyymmdd_L05_F51.csv) BM Backup --------------------------------------------------------------------
def Loobos_Read_NL_Loo_BM_Backup   (t1,t2,keys=None,get_keys=False,datapath=None, API=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath

    datapath             = getDatapath(datapath)
    timestamp_str   = 'TIMESTAMP'
    all_keys             = ["TIMESTAMP","SW_IN_2_1_1","TA_3_1_1","RH_2_1_1","P_2_1_1"]
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
        
    if API == True:      # For the API we want one entire day file. The dayfiles start 20s after midnight and run until midnight.
        t_shift          = np.timedelta64(20,'s')
    else:
        t_shift          = 0
 
    t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(20,'s')).astype(np.datetime64)
#    t                    = np.arange(t1,t2,np.timedelta64(20,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm                   = [] 
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM-Backup','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L05_F51.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM-Backup', 'raw','NL-Loo_BM_L05_F51.dat')
        filename_L51         = os.path.join(datapath,'NL-Loo_BM-Backup','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L51_F51.csv' %(yy,mm,dd))
        filename_L51_tmp     = os.path.join(datapath,'NL-Loo_BM-Backup', 'raw','NL-Loo_BM_L051_F51.dat')
        # Try filename
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_L51):          # Try filename_L51
            filename = filename_L51
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_L51_tmp):         # Try filename_L51_tmp
            filename     = filename_L51_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:          # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            

        if file_success:
            converters = {key: float for key in all_keys if not key == timestamp_str}
            if   (filename == filename_tmp or filename == filename_L51_tmp):
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y-%m-%d %H:%M:%S',index_col=timestamp_str,converters=converters,usecols=keys))
            else:
                bm.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S',index_col=timestamp_str,converters=converters,usecols=keys))
    if len(bm) > 0:                
        bm          = pd.concat(bm, axis=0,ignore_index=False)
        bm          = bm[~bm.index.duplicated()]
        bm          = bm.reindex(timeIndex)#.ffill()
    else:
        bm = pd.DataFrame(index=timeIndex, columns=keys)

    return bm

###--- (NL-Loo_ST_yyyymmdd_L03_F31.csv) ST Storage Profile ----------------------------------------------------------
def Loobos_Read_NL_Loo_ST          (t1,t2,keys=None,get_keys=False, datapath=None):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2 , dateparse_bm3, getDatapath 

    def valid_int(y):
        try:
            return(np.int32(y))
        except:
            return np.nan

    all_keys = ["TIMESTAMP","Level","LI_850_cell_temp","LI_850_cell_press","LI_850_dewpoint","LI_850_H2O","LI_850_CO2","SMC_flow"]
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
    datapath             = getDatapath(datapath)        
    t                    = np.arange(t1+np.timedelta64(1,'s'),t2+np.timedelta64(1,'s'),np.timedelta64(1,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    st                   = [] 
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_ST','%04d'%yy, 'NL-Loo_ST_%4d%02d%02d_L03_F31.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_ST', 'raw','NL-Loo_ST_L03-Profile.dat')
        # Try filename
#       print('ST: looking for :', filename)
        if os.path.exists(filename):
            print('1. File found: ',os.path.exists(filename),filename)
            file_success = True
        # Try filename_tmp
        elif os.path.exists(filename_tmp):
            filename     = filename_tmp
            print('2. File found: ',os.path.exists(filename),filename)
            file_success = True
        # Nothing seems to work
        else:
            print('3. File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            
        if file_success:
            converters = {'Level': valid_int, 'LI_850_cell_temp': float, 'LI_850_cell_press': float, 'LI_850_dewpoint': float, 'LI_850_H2O': float, 'LI_850_CO2': float, 'SMC_flow': float, 'CO2_cal_target': float, 'LI_850_CO2_cal': float}
            if   filename == filename_tmp:               # Raw data file today
                #ESG_SB_20240515+ Added try except switch for temporarily broken/removed H2O profile
                try:
                    st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y-%m-%d %H:%M:%S',index_col='TIMESTAMP',converters=converters,usecols=keys))
                except:
                    st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y-%m-%d %H:%M:%S',index_col='TIMESTAMP',
                                converters={'Level': valid_int, 'LI_850_cell_temp': float, 'LI_850_cell_press': float, 'LI_850_CO2': float, 'SMC_flow': float, 'CO2_cal_target': float, 'LI_850_CO2_cal': float},
                                usecols=["TIMESTAMP","Level","LI_850_cell_temp","LI_850_cell_press","LI_850_CO2","SMC_flow"]))                    
                                
                #ESG_SB_20240515- Added try except switch for temporarily broken/removed H2O profile
            elif (day <= np.datetime64('2022-10-04')):
                st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y-%m-%d %H:%M:%S',index_col='TIMESTAMP',converters=converters,usecols=keys))
            elif (day <= np.datetime64('2022-11-05')):
                st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S.%f',index_col='TIMESTAMP',converters=converters,usecols=keys))
            elif (day <= np.datetime64('2022-11-06')):
                st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S',index_col='TIMESTAMP',converters=converters,usecols=keys))
            elif (day <= np.datetime64('2022-11-25')):
                st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S.%f',index_col='TIMESTAMP',converters=converters,usecols=keys))
            else: #if (day >= np.datetime64('2022-11-26')) :        # format YYYMMDDHHMMSS.ff\
                #ESG_SB_20240515+ Added try except switch for temporarily broken/removed H2O profile
                try:
                    st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S',index_col='TIMESTAMP',converters=converters,usecols=keys))
                except:
                    st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S',index_col='TIMESTAMP',
                                converters={'Level': valid_int, 'LI_850_cell_temp': float, 'LI_850_cell_press': float, 'LI_850_CO2': float, 'SMC_flow': float, 'CO2_cal_target': float, 'LI_850_CO2_cal': float},
                                usecols=["TIMESTAMP","Level","LI_850_cell_temp","LI_850_cell_press","LI_850_CO2","SMC_flow"]))
                #ESG_SB_20240515- Added try except switch for temporarily broken/removed H2O profile
    if len(st) > 0:
        st   = pd.concat(st, axis=0,ignore_index=False)
        st   = st[~st.index.duplicated()]
        st   = st.reindex(timeIndex)#.ffill()
    else:
        st  = pd.DataFrame(index=timeIndex, columns=keys)

    return st

###--- (NL-Loo_ST_yyyymmdd_L03_F32.csv) ST Storage Profile ----------------------------------------------------------
def Loobos_Read_NL_Loo_ST_Cal      (t1,t2,keys=None,get_keys=False, datapath=None, getCalLine=False):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2 , dateparse_bm3, getDatapath 

    def valid_int(y):
        try:
            return(np.int32(y))
        except:
            return np.nan

    all_keys             = ["TIMESTAMP","CO2","H2O","LEVEL","FLOW_VOLRATE","T_CELL","PRESS_CELL"]
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
    datapath             = getDatapath(datapath)        
    t                    = np.arange(t1+np.timedelta64(1,'s'),t2+np.timedelta64(1,'s'),np.timedelta64(1,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    st                   = [] 
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_ST','%04d'%yy, 'NL-Loo_ST_%4d%02d%02d_L03_F32.csv' %(yy,mm,dd))
        # Try filename
        print('ST: looking for :', filename)
        if os.path.exists(filename):
            print('1. File found: ',os.path.exists(filename),filename)
            file_success = True
        # Nothing seems to work
        else:
            print('3. File found: ',os.path.exists(filename),filename)
            file_success = False
            
        if file_success:
            converters = {                    'T_CELL': float, 'PRESS_CELL': float, 'H2O': float, 'CO2': float, 'FLOW_VOLRATE': float}
            st.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',converters=converters,usecols=keys, encoding='cp1252',date_format='%Y%m%d%H%M%S'))
            
            if ((getCalLine == True) & (iday == 0)):
                with open(os.path.join(datapath,filename)) as infile:
                    hdr = infile.readline()
  

    if len(st) > 0:
        st   = pd.concat(st, axis=0,ignore_index=False)
        st   = st[~st.index.duplicated()]
        st   = st.reindex(timeIndex)#.ffill()
        st['LEVEL'] = st['LEVEL'].astype(pd.Int64Dtype())
    else:
        st  = pd.DataFrame(index=timeIndex, columns=keys)
        
    if (getCalLine == True):    
        return st, hdr
    else:
        return st

### --- Loobos_ST_calibration
def Loobos_ST_calibration          (t1,t2,st,bm,datapath=None):
    import pandas as pd
    import numpy as np
    import os
    from Loobos_Toolbox_NewTower import Loobos_Read_NL_Loo_BM, Loobos_Read_NL_Loo_ST #Loobos_Read_NL_Loo_BM_Soil, Loobos_Read_GHG_EddyPro_full, Loobos_ReadGHG_Raw, Loobos_Read_NL_Loo_Stat, Loobos_Read_NL_Loo_ST, Loobos_Read_NL_Loo_AQ, satvap
    TEST = False # Set to False when used as function
   #TEST = True
    if TEST:
        t1 = np.datetime64('2023-02-23')
        t2 = np.datetime64('2023-03-20')
        t1s = np.arange(t1,t2,np.timedelta64(1,'D')).astype(np.datetime64)
        ndaysloop = len(t1s)

   #datapath    = getDatapath(datapath)
   #datapath_ST = os.path.join(datapath,'NL-Loo_ST')
    datapath_ST = os.path.join('C:\\','DATA','NL-Loo_ST')
    if not os.path.exists(datapath_ST):
        datapath_ST = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','NL-Loo_ST')

    #=================================================#
    # Analysis report Calibration flasks 9 Sept 2020  #
    # Flask      CO2     CH4     CO H2O               #
    # Units      ppm     ppb    ppb ppm               #
    # JJ06914 387.91  1954.6  116.3  12               #
    # JJ06919 418.50  2069.6  146.4  17               #
    # JJ06920 464.62  2476.4  325.5  14               #
    # JJ06922 367.93  1990.4  166.7  22               #
    #=================================================#
    caldata_CO2 = {'t_start'   : np.array(['2020-09-09 00:00:00','2023-02-08 13:00:00', '2100-01-01 00:00:00']).astype(np.datetime64),
                   't_end'     : np.array(['2023-02-08 13:00:00','2100-01-01 00:00:00', '2200-01-01 00:00:00']).astype(np.datetime64), 
                   'CO2_12'    :          [              464.64 ,              464.64 ,               550.00 ],    # ppm JJ06920  =  JJ6920
                   'CO2_13'    :          [              418.50 ,              367.93 ,                 0.00 ],    # ppm JJ06919 --> JJ6922
                   'H2O_12'    :          [               14.00 ,               14.00 ,                 0.00 ],    # ppm JJ06920  =  JJ6920
                   'H2O_13'    :          [               17.00 ,               22.00 ,                 0.00 ]}    # ppm JJ06919 --> JJ6922
    caldata_CO2     = pd.DataFrame(data=caldata_CO2)
    calcoef_H2O     = np.array([1.12,    0.02 ]) # Calibration with LI610 on 2023-03-14
    calcoef_H2O     = np.array([1.13,   -0.02 ]) # Calibration with LI610 on 2023-03-29
    calcoef_H2O     = np.array([1.0102,  0.018]) # Calibration with LI610 on 2023-04-03
    calcoef_H2O     = np.array([1.0000,  0.000]) # After 2024-05-28
    

    t           = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    HH          = [t[it].astype(object).hour for it in range(len(t))]
    if TEST:
        if not 'progress' in globals(): progress = list()
        if not (t1 in progress):
            st      = Loobos_Read_NL_Loo_ST(       t1,t2,keys=None)
            bm      = Loobos_Read_NL_Loo_BM(       t1,t2,keys=None)
            
            t_st    = st.index
            progress = [t1,]
    else:
        t_st = st.index
        
    # profile data analysis
    nlevels     = 12
    z           = [38.2, 30, 22.1, 15.7, 11.2, 7.4, 4.5, 2.4, 1.5, 0.4, 0.1]
    dz          = np.diff(z)
    dCdz_mean   = np.zeros((48,10))
    dCdz_std    = np.zeros((48,10))
    eq_seconds  = [15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59]


    # Find the right row in caldata_CO2 and set calibration target concentrations
    ncal = len(caldata_CO2)
    for ical in range(ncal):
        # set target concentrations for channel 12: 'low'
        I = ( (st['Level'] == 12) & (st.index > caldata_CO2.loc[ical,'t_start']) & (st.index <= caldata_CO2.loc[ical,'t_end']) )
        if len(st[I]) > 0:
            print('Using caldata_CO2 for t_start=%s and t_end=%s'%(caldata_CO2.loc[ical,'t_start'],caldata_CO2.loc[ical,'t_end']))
            st.loc[I,'CO2_cal_target'] = caldata_CO2.loc[ical,'CO2_12']
            st.loc[I,'H2O_cal_target'] = caldata_CO2.loc[ical,'H2O_12']

        # set target concentrations for channel 13: 'high'
        I = ( (st['Level'] == 13) & (st.index > caldata_CO2.loc[ical,'t_start']) & (st.index <= caldata_CO2.loc[ical,'t_end']) )
        if len(st[I]) > 0:
            st.loc[I,'CO2_cal_target'] = caldata_CO2.loc[ical,'CO2_13']
            st.loc[I,'H2O_cal_target'] = caldata_CO2.loc[ical,'H2O_13']

    if False:                                                                         # Set to True when calibration channels were not connected yet 
       I                          = (st['Level'] == 12.0)                             # Set 'observed' concentrations for 'Low'
       st.loc[I,'LI_850_CO2']     = 13.0+np.random.randint(0,100,size=(nI,))/10000    # Set 'observed' concentration for channel 13 (for as long as the cylinder is not connected) to a value around 13 ppm
       I                          = (st['Level'] == 13.0)                             # Set 'observed' concentrations for 'High'
       st.loc[I,'LI_850_CO2']     = 523.0+np.random.randint(0,100,size=(nI,))/10000   # Set 'observed' concentration for channel 13 (for as long as the cylinder is not connected) to a value around 523 ppm

    #Calibration is performed two times per day, but this can change. Find calibration blocks and their start times. Apply the calibration from start time of one block until the start time of the next block.
    #    0123                 3    3                 3      332 1   2                 21
    I                  = ((((st['Level'] >= 12) != (st['Level'] >= 12).shift()) ) & (st['Level'] >= 12))
    t_block_start      = st.index[I]
    
    # sometimes it detects an extra start time, eg. if there is a line of NaN's. Reject start times with minutes larger than 0
    I                  = ((t_block_start.minute == 0) & (t_block_start.second < 5))
    t_block_start      = t_block_start[I]
    
    I                  = ((((st['Level'] >= 12) != (st['Level'] >= 12).shift()) ) & (st['Level']  < 12))
    t_block_end        = st.index[I]

    # Read archive of calibration reports
    calreport_filename = os.path.join(datapath_ST,'calreport.csv')
    calreport          = pd.read_csv(calreport_filename,index_col = 'TIMESTAMP',parse_dates=['TIMESTAMP','t_start','t_end'])
    calreport.index    = pd.to_datetime(calreport.index)
    calreport['t_end'] = pd.to_datetime(calreport['t_end'],format = '%Y%m%d%H%M%S')

    nblocks = len(t_block_start)
    print('nblocks=',nblocks)
    for ib in range(nblocks):
        try:
            I                        = ((st.index >= t_block_start[ib]) & (st.index < t_block_end[ib]) & (t_st.second.isin(eq_seconds)) & (np.isnan(st['LI_850_CO2']) == False)) # Find rows when calibration gasses are running and >= 15 seconds after switching channels
            p                        = np.polyfit(st.loc[I,'CO2_cal_target'],st.loc[I,'LI_850_CO2'],1)  # find regression coefficients
            print(p)
        except:
            p = [-9999,-9999]
            print('N.B.: CO2 calibration failed')
        
        #--- Check if calibration coefficient is within acceptable ranges
        slope_min  =  0.90
        slope_max  =  1.10   #henk changed from 1.0 to 1.1 2024-22-05
        offset_min = -30.0   #     changed from 0 to -30
        offset_max =  40.0
        print('ST Calibration: (slope, offset) = (%7.4f, %7.4f)'%(p[0],p[1]))
        if ((p[0] < slope_min) | (p[0] > slope_max) | (p[1] < offset_min) | (p[1] > offset_max)):
            # False calibration detected. Reset by last realistic value.
            print('False calibration detected')
            print('t_start = ',t_block_start[ib])
            dt = (calreport['t_start'] - t_block_start[ib]).dt.total_seconds()/86400
            print('Earliest alternative:')
            print(calreport.loc[dt[dt < -0.4].idxmax()])
            p = calreport.loc[dt[dt < -0.4].idxmax()][['slope','offset']].values
        else:
            print('...ST Calibration coefficients accepted.')
        t_end                        = t_block_start[ib+1] if ib < nblocks-1 else st.index[-1]+pd.Timedelta(seconds=1)     # find end of block (when a new calibration block starts

        print(t_block_start[ib],t_end)
        I                            = ((st.index >= t_block_start[ib]) & (st.index < t_end))          # find all samples in that block
        st.loc[I,'LI_850_CO2_cal']   = (st.loc[I,'LI_850_CO2']-p[1])/p[0]                              # apply calibration
        st['LI_850_CO2_cal']         = st['LI_850_CO2_cal'].round(4)

        # Print table to show how well the calibration was applied.
        print('time %s-%s: y = %7.4f x + %7.2f'%(t_block_start[ib],t_end,p[0],p[1]))
        I12                          = (st['Level'] == 12)
        print('level 12: obs   :     %7.2f (+/- %7.2f)'%(st.loc[I12,'LI_850_CO2'    ].mean(),st.loc[I12,'LI_850_CO2'    ].std()))
        print('level 12: target:     %7.2f (+/- %7.2f)'%(st.loc[I12,'CO2_cal_target'].mean(),st.loc[I12,'CO2_cal_target'].std()))
        print('level 12: calibrated: %7.2f (+/- %7.2f)'%(st.loc[I12,'LI_850_CO2_cal'].mean(),st.loc[I12,'LI_850_CO2_cal'].std()))
        I13                          = (st['Level'] == 13)
        print('level 13: obs   :     %7.2f (+/- %7.2f)'%(st.loc[I13,'LI_850_CO2'    ].mean(),st.loc[I13,'LI_850_CO2'    ].std()))
        print('level 13: target:     %7.2f (+/- %7.2f)'%(st.loc[I13,'CO2_cal_target'].mean(),st.loc[I13,'CO2_cal_target'].std()))
        print('level 13: calibrated: %7.2f (+/- %7.2f)'%(st.loc[I13,'LI_850_CO2_cal'].mean(),st.loc[I13,'LI_850_CO2_cal'].std()))
        I                            = (st['Level'] ==  1)
        print('level  1: obs   :     %7.2f (+/- %7.2f)'%(st.loc[I,'LI_850_CO2'    ].mean(),st.loc[I,'LI_850_CO2'    ].std()))
        print('level  1: target:     %7.2f (+/- %7.2f)'%(st.loc[I,'CO2_cal_target'].mean(),st.loc[I,'CO2_cal_target'].std()))
        print('level  1: calibrated: %7.2f (+/- %7.2f)'%(st.loc[I,'LI_850_CO2_cal'].mean(),st.loc[I,'LI_850_CO2_cal'].std()))
     
        idx = pd.to_datetime(t_block_start[ib])
        calreport.loc[idx] = {'t_start':np.datetime64(t_block_start[ib]), 't_end':np.datetime64(t_end),'CO2 target 12':st.loc[I12,'CO2_cal_target'].mean(),'CO2 target 13':st.loc[I13,'CO2_cal_target'].mean(),'slope':p[0],'offset':p[1]}
    calreport['CO2 target 12'] = calreport['CO2 target 12'].round(decimals=2)
    calreport['CO2 target 13'] = calreport['CO2 target 13'].round(decimals=2)
    calreport['slope'        ] = calreport['slope'        ].round(decimals=4)
    calreport['offset'       ] = calreport['offset'       ].round(decimals=4)

    calreport = calreport[~calreport.index.duplicated()]
    calreport = calreport.sort_index()
    calreport.to_csv(calreport_filename,sep=',',float_format='%8.4f')

    # Apply H2O calibration
    if not 'LI_850_H2O' in st.keys():
        st['LI_850_H2O'] = np.nan
    st['LI_850_H2O_cal']         = st['LI_850_H2O']*calcoef_H2O[0]+calcoef_H2O[1]                               # apply H2O calibration
    st['LI_850_H2O_cal']         = st['LI_850_H2O_cal'].round(4)
    if not 'LI_850_CO2_cal' in st.keys(): st['LI_850_CO2_cal'] = np.nan
        
    # Write output file (one per day)
    st_out            = st[['LI_850_CO2_cal', 'LI_850_H2O_cal', 'Level', 'SMC_flow', 'LI_850_cell_temp', 'LI_850_cell_press']]
    st_out            = st_out.rename(columns={'LI_850_CO2_cal':'CO2', 'LI_850_H2O_cal':'H2O', 'Level': 'LEVEL', 'SMC_flow': 'FLOW_VOLRATE', 'LI_850_cell_temp': 'T_CELL', 'LI_850_cell_press': 'PRESS_CELL'})  
    st_out.index.name = 'TIMESTAMP'
    
    # Mask calibration channels
    I                 = (st_out['LEVEL'] >= 12)
    st_out.loc[I,'CO2'] = np.nan
    st_out.loc[I,'H2O'] = np.nan

    days              = np.arange(t1,t2,np.timedelta64(1,'D'))
    ndays             = len(days)
    for iday in range(ndays):
        day = np.datetime64(days[iday],'D')
        I   = (calreport.index.date == day) # assume two calibrations per day
        caltmp = calreport.loc[I]
        if len(caltmp) > 0:
            line1  = "CO2 Target Level 12: %8.4f ppm and Level 13: %8.4f ppm; "%(caltmp.iloc[0]['CO2 target 12'],caltmp.iloc[0]['CO2 target 13'])
            for i in range(len(caltmp)):
                line1 += "%s: Calibration curve : CO2_cal = (CO2_obs - %8.4f) / %8.4f; "%(caltmp.index[i],caltmp.iloc[i]['offset'],caltmp.iloc[i]['slope'])
            line1 += "H2O_cal = (H2O_obs * %8.4f + %8.4f; "%(calcoef_H2O[0],calcoef_H2O[1])
            
            header = ['%s\n'%line1,
                      '"TIMESTAMP","CO2","H2O","LEVEL","FLOW_VOLRATE","T_CELL","PRESS_CELL"\n',
                      '"TS","\µmol mol-1","mmol mol-1","#","litres min-1","°C","kPa"\n',
                      '"","Smp","Smp","","Smp","Smp","Smp"\n']
            
            outfilename = os.path.join(datapath_ST,'%4d'%day.astype(object).year,'NL-Loo_ST_%4d%02d%02d_L03_F32.csv'%(day.astype(object).year,day.astype(object).month,day.astype(object).day))
            with open(outfilename, 'w') as fp :                #Write the orignal header back
                fp.writelines(header) 
                fp.close()
            I = ((st_out.index.date == day) | ( (st_out.index.date == day+np.timedelta64(1,'D') ) & (st_out.index.hour == 0) * (st_out.index.minute == 0) & (st_out.index.second == 0)))
            if TEST:
                print('outfile %s not written.'%outfilename)
            else:
                st_out[I].to_csv(outfilename, sep=',', mode='a', header=0, index=True,na_rep='"NaN"',quotechar="'",date_format ='"%Y%m%d%H%M%S"')
    return st_out

###--- (NL-Loo_ST_yyyymmdd_L02_F11.csv) BM Meteo Stat ---------------------------------------------------------------
def Loobos_Read_NL_Loo_Stat        (t1,t2,keys=None,get_keys=False,datapath=None):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath

    #TIMESTAMP Changed
    term_date_1          = date(2022, 8,31)
    term_date_2          = date(2022,10, 5)
    if ((t1 > term_date_1) & (t1 <= term_date_2)):
         timestamp_str   = 'Time_Stamp'
    else:
         timestamp_str   = 'TIMESTAMP'
    all_keys = [timestamp_str,"panel_temp","Batt12v","Pitch","Roll","Stat_230V"]

    if keys is None:
        keys = all_keys
    if get_keys == True:
        return all_keys
    datapath             = getDatapath(datapath)
    t                    = np.arange(t1,t2,np.timedelta64(5,'m')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)

    timeIndex            = pd.DatetimeIndex(t)
    stat                 = [] 
    for iday in range(ndays):
        day               = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM','%04d'%yy, 'NL-Loo_Stat_%4d%02d%02d_L02_F11.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM', 'Raw','NL-Loo_BM_L02-meteo_logger_stat.dat')
        print('File found: ',os.path.exists(filename),filename)
        if os.path.exists(filename):
            file_success = True
        elif os.path.exists(filename_tmp):
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:
            file_success = False
            
        if file_success:
            if   ( (day <= np.datetime64('2022-08-30')) | (filename == filename_tmp)):
                stat.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y-%m-%d %H:%M:%S',index_col=timestamp_str))
            elif day <= np.datetime64('2022-08-31'):
                print('In Loobos_Read_NL_Loo_Stat: this date not implemented yet')
            elif day <= np.datetime64('2022-11-06'):
                stat.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S.%f',index_col=timestamp_str))
            else:
                stat.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S',index_col=timestamp_str))
    if len(stat) > 0:
        stat        = pd.concat(stat, axis=0,ignore_index=False)
        stat        = stat[~stat.index.duplicated()]
        stat        = stat.reindex(timeIndex)
        if not 'Stat_230V' in stat.keys():
            stat['Stat_230V'] = np.nan
    else:
        stat        = pd.DataFrame(index=timeIndex, columns=keys)

    return stat

###--- NO2 files (NO/NO2) --------------------------------------------------------------------------------------------
def Loobos_Read_NL_Loo_NO2         (t1,t2,keys=None,datapath=None):
   #from Loobos_Toolbox_NewTower import dateparse_aq, getDatapath
#   datapath   = 'w:\\ESG\\DOW_MAQ\\MAQ_Archive\\loobos_archive\\NL-Loo_NO2\\'
    datapath             = getDatapath(datapath)
    t                    = np.arange(t1,t2,np.timedelta64(5,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)-1
    timeIndex            = pd.DatetimeIndex(t)
    NO2                  = list()
    for iday in range(ndays):
        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_NO2','%2d'%yy,'NL-Loo_NO2_%4d%02d%02d_L08_F81.csv' %(yy,mm,dd))
        
        if day > np.datetime64('2024-01-01'):
            all_keys   = ['date','time','LGRtime','wave3','LGR_NO2ppb','wave5','Cell_Pressure_Top','wave7','wave8','wave9','wave10','wave11','wave12','wave13','wave14','wave15','wave16','wave17','wave18','wave19','wave20','wave21','wave22','wave23','wave24']
        elif ((day < np.datetime64('2023-01-30')) | (day >= np.datetime64('2023-02-06')) ):
            all_keys   = ['date','time',        'NO2','NO','NOx','T_cell','p_cell','F_cell','F_O3cell','V','V_O3','T_scrub','ErrorByte','date_sensor','time_sensor','Status']
        else:
            all_keys   = ['date','time','recno','NO2','NO','NOx','T_cell','p_cell','F_cell','F_O3cell','V','V_O3','T_scrub','ErrorByte','date_sensor','time_sensor','Status']
        
        print('NO2: ',filename)
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:          # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename)
            file_success = False
            
        if file_success:
           #NO2_tmp = pd.read_csv(os.path.join(datapath,filename),sep=',',header=None,names=all_keys,parse_dates=[['date','time']],date_format='%d-%m-%Y %H:%M:%S.%f',index_col='date_time')
            NO2_tmp = pd.read_csv(os.path.join(datapath,filename),sep=',',header=None,names=all_keys)
            NO2_tmp.index = pd.to_datetime(NO2_tmp['date'] + NO2_tmp['time'],format='%d-%m-%Y%H:%M:%S.%f')
            NO2.append(NO2_tmp)#,converters=converters))
            
    if len(NO2) > 0:
        NO2         = pd.concat(NO2, axis=0,ignore_index=False)
        NO2.index   = NO2.index.round('1s')
        NO2         = NO2[~NO2.index.duplicated()]  
        timeIndex   = pd.DatetimeIndex(np.arange(NO2.index[0],NO2.index[-1],np.timedelta64(1,'s')).astype(np.datetime64))
        NO2         = NO2.reindex(timeIndex)
    else:
        NO2 = pd.DataFrame(index = timeIndex, columns=all_keys)
        
    NO2.index.name = 'TIMESTAMP'
    return NO2

###--- function satvap ----------------------------------------------------------------------------------------------
def satvap(T):
    # Calculate saturation vapour pressure (kPa)
    es_0 = 0.6107 # kPa
    a_w  = 7.5    # -   parameters over water
    b_w  = 237.3  # oC
    a_i  = 9.5    # -   parameters over ice
    b_i  = 265.5  # oC
    
    I    = (T > 100)
    T[I] = T[I] - 273.15 # Convert to deg Celcius
    
    es_w = es_0 * pow(10,(a_w*T)/(b_w+T)) # saturation vapour pressure over water
    es_i = es_0 * pow(10,(a_i*T)/(b_i+T)) # saturation vapour pressure over ice
    
    es   = es_w
    I    = (T < 0)
    es[I]= es_i[I]
    return es    

###--- (NL-Loo_BM_yyyymmdd_L04_F41.csv) BM Soil OLD -----------------------------------------------------------------
def Loobos_Read_NL_Loo_BM_Soil_old (t1,t2,keys=None,get_keys=False,datapath=None):
    from Loobos_Toolbox_NewTower import dateparse_bm1, dateparse_bm2, dateparse_bm3, getDatapath 

    #TIMESTAMP Changed
#    term_date_1         = date(2022, 8,31)
#    term_date_2         = date(2022,10, 5)
#    if ((t1 > term_date_1) & (t1 <= term_date_2)):
#         timestamp_str  = 'Time_Stamp'
#    else:
#         timestamp_str  = 'TIMESTAMP'
    timestamp_str        = "TIMESTAMP"
#   datapath         = 'w:\\ESG\\DOW_MAQ\\MAQ_Archive\\loobos_archive\\NL-Loo_BM-Soil\\' 
    datapath             = getDatapath(datapath)
    if t1 < np.datetime64('2023-01-12'):
        dt = 20
    else:
        dt = 60
   
    t                    = np.arange(t1,t2,np.timedelta64(dt,'s')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    bm_soil              = [] 
    for iday in range(ndays):

        day              = days[iday] #+ 1  # add 1 day, because the data from day N are stored in the file of N+1
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_BM-Soil','%4d'%yy,'NL-Loo_BM_%4d%02d%02d_L04_F41.csv' %(yy,mm,dd))
        filename_tmp     = os.path.join(datapath,'NL-Loo_BM-Soil','raw','NL-Loo_BM-L03-Soil.dat')
        
        if   day <= pd.to_datetime('2023-01-31'):
            all_keys             = [timestamp_str,"HxF_sig(1)","HxF_sig(2)","HxF_sig(3)","HxF_sig(4)","HxF_cal(1)","HxF_cal(2)","HxF_cal(3)","HxF_cal(4)","TS_1_2_1","TS_1_1_1","TS_2_2_1","TS_2_1_1","TS_3_1_1","TS_3_2_1","TS_4_1_1","TS_4_2_1","CS655(1,1)","CS655(1,2)","TS_1_2_2","CS655(1,4)","CS655(1,5)","CS655(1,6)","CS655(2,1)","CS655(2,2)","TS_1_3_1","CS655(2,4)","CS655(2,5)","CS655(2,6)","CS655(3,1)","CS655(3,2)","TS_1_4_1","CS655(3,4)","CS655(3,5)","CS655(3,6)","CS655(4,1)","CS655(4,2)","TS_1_5_1","CS655(4,4)","CS655(4,5)","CS655(4,6)","CS655(5,1)","CS655(5,2)","TS_1_6_1","CS655(5,4)","CS655(5,5)","CS655(5,6)","CS655(6,1)","CS655(6,2)","TS_3_2_2","CS655(6,4)","CS655(6,5)","CS655(6,6)","CS655(7,1)","CS655(7,2)","TS_2_2_2","CS655(7,4)","CS655(7,5)","CS655(7,6)","CS655(8,1)","CS655(8,2)","TS_2_3_1","CS655(8,4)","CS655(8,5)","CS655(8,6)","CS655(9,1)","CS655(9,2)","TS_2_4_1","CS655(9,4)","CS655(9,5)","CS655(9,6)","CS655(10,1)","CS655(10,2)","TS_2_5_1","CS655(10,4)","CS655(10,5)","CS655(10,6)","CS655(11,1)","CS655(11,2)","TS_2_6_1","CS655(11,4)","CS655(11,5)","CS655(11,6)","CS655(12,1)","CS655(12,2)","TS_4_2_2","CS655(12,4)","CS655(12,5)","CS655(12,6)"]
            keys                 = all_keys
        elif day <= pd.to_datetime('2023-02-20'):
            all_keys             = [timestamp_str,"HxF_sig(1)","HxF_sig(2)","HxF_sig(3)","HxF_sig(4)","HxF_cal(1)","HxF_cal(2)","HxF_cal(3)","HxF_cal(4)","TS_1_2_1","TS_1_1_1","TS_2_2_1","TS_2_1_1","TS_3_1_1","TS_3_2_1","TS_4_1_1","TS_4_2_1","CS655(1,1)","CS655(1,2)","TS_1_2_2","CS655(1,4)","CS655(1,5)","CS655(1,6)","CS655(2,1)","CS655(2,2)","TS_1_3_1","CS655(2,4)","CS655(2,5)","CS655(2,6)","CS655(3,1)","CS655(3,2)","TS_1_4_1","CS655(3,4)","CS655(3,5)","CS655(3,6)","CS655(4,1)","CS655(4,2)","TS_1_5_1","CS655(4,4)","CS655(4,5)","CS655(4,6)","CS655(5,1)","CS655(5,2)","TS_1_6_1","CS655(5,4)","CS655(5,5)","CS655(5,6)","CS655(6,1)","CS655(6,2)","TS_3_2_2","CS655(6,4)","CS655(6,5)","CS655(6,6)","CS655(7,1)","CS655(7,2)","TS_2_2_2","CS655(7,4)","CS655(7,5)","CS655(7,6)","CS655(8,1)","CS655(8,2)","TS_2_3_1","CS655(8,4)","CS655(8,5)","CS655(8,6)","CS655(9,1)","CS655(9,2)","TS_2_4_1","CS655(9,4)","CS655(9,5)","CS655(9,6)","CS655(10,1)","CS655(10,2)","TS_2_5_1","CS655(10,4)","CS655(10,5)","CS655(10,6)","CS655(11,1)","CS655(11,2)","TS_2_6_1","CS655(11,4)","CS655(11,5)","CS655(11,6)","CS655(12,1)","CS655(12,2)","TS_4_2_2","CS655(12,4)","CS655(12,5)","CS655(12,6)","Press","Ts_1_7_1","CS451_2(1)","CS451_2(2)"]
            keys                 = all_keys
        else: #if day >= pd.to_datetime('2023-02-21'):
            all_keys             = ["TIMESTAMP","TS_1_1_1","TS_1_2_1","TS_1_2_2","TS_1_3_1","TS_1_4_1","TS_1_5_1","TS_1_6_1","TS_1_7_1","TS_2_1_1","TS_2_2_1","TS_2_2_2","TS_2_3_1","TS_2_4_1","TS_2_5_1","TS_2_6_1","TS_2_7_1","TS_3_1_1","TS_3_2_1","TS_3_2_2","TS_4_1_1","TS_4_2_1","TS_4_2_2","SWC_1_1_1","SWC_1_2_1","SWC_1_3_1","SWC_1_4_1","SWC_1_5_1","SWC_2_1_1","SWC_2_2_1","SWC_2_3_1","SWC_2_4_1","SWC_2_5_1","SWC_3_1_1","SWC_4_1_1","G_1_1_1","G_ISCAL_1_1_1","G_2_1_1","G_ISCAL_2_1_1","G_3_1_1","G_ISCAL_3_1_1","G_4_1_1","G_ISCAL_4_1_1","WTD_1_1_1","WTD_2_1_1","SWC_IU_1_1_1","SWC_IU_1_2_1","SWC_IU_1_3_1","SWC_IU_1_4_1","SWC_IU_1_5_1","SWC_IU_2_1_1","SWC_IU_2_2_1","SWC_IU_2_3_1","SWC_IU_2_4_1","SWC_IU_2_5_1","SWC_IU_3_1_1","SWC_IU_4_1_1","G_IU_1_1_1","G_IU_2_1_1","G_IU_3_1_1","G_IU_4_1_1","G_SF_1_1_1","G_SF_2_1_1","G_SF_3_1_1","G_SF_4_1_1","WCP_1_1_1","WCP_2_1_1"]
            keys                 = all_keys

        if keys is None:
           keys             = all_keys
        if get_keys == True:
            return all_keys

        if os.path.exists(filename):               # Try filename
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        elif os.path.exists(filename_tmp):         # Try filename_tmp
            filename     = filename_tmp
            print('File found: ',os.path.exists(filename),filename)
            i       = keys.index('TS_1_7_1')
            keys[i] =         'T_WTD_1_7_1'
            i       = keys.index('TS_2_7_1')
            keys[i] =         'T_WTD_2_7_1'
            file_success = True
        else:                                      # Nothing seems to work
            print('File found: ',os.path.exists(filename),filename,';  File found: ',os.path.exists(filename_tmp),filename_tmp)
            file_success = False
            
        if file_success:
            converters = {key: float for key in all_keys if not key == timestamp_str}
            if filename == filename_tmp: 
                bm_soil.append(    pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y-%m-%d %H:%M:%S',index_col=timestamp_str,converters=converters,usecols=keys))
            else:
                if day <= np.datetime64('2022-11-26'):
                    bm_soil.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S.%f',index_col=timestamp_str,converters=converters,usecols=keys))
                else:
                    bm_soil.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=[timestamp_str],date_format='%Y%m%d%H%M%S',index_col=timestamp_str,converters=converters,usecols=keys))
    if len(bm_soil) > 0:
        bm_soil          = pd.concat(bm_soil, axis=0,ignore_index=False)
        bm_soil          = bm_soil[~bm_soil.index.duplicated()]
        bm_soil          = bm_soil.reindex(timeIndex)#.ffill()
    else:
        bm_soil = pd.DataFrame(index=timeIndex, columns=keys)
    
#    if day <= pd.to_datetime('2023-02-19):
#        if not 'Press'       in bm_soil.keys():  bm_soil['Press'     ] = np.nan
#        if not 'Ts_1_7_1'    in bm_soil.keys():  bm_soil['Ts_1_7_1'  ] = np.nan
#        if not 'CS451_2(1)'  in bm_soil.keys():  bm_soil['CS451_2(1)'] = np.nan
#        if not 'CS451_2(2)'  in bm_soil.keys():  bm_soil['CS451_2(2)'] = np.nan

    # On 21 Feb 2023, the column names changed. On that day, both the old and the new column names appear in the file. The first part has the old column names, the new part the old ones. Fix this and drop the old column names, otherwise there will be duplicates.
    if (('HxF_cal(1)' in bm_soil.keys()) & ('G_SF_1_1_1' in bm_soil.keys())):
        I = ( (np.isnan(bm_soil['G_SF_1_1_1'])) & (~np.isnan(bm_soil['HxF_cal(1)' ]))); bm_soil.loc[I,'G_SF_1_1_1'] = bm_soil.loc[I,'HxF_cal(1)' ]; bm_soil = bm_soil.drop(['HxF_cal(1)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_SF_2_1_1'])) & (~np.isnan(bm_soil['HxF_cal(2)' ]))); bm_soil.loc[I,'G_SF_2_1_1'] = bm_soil.loc[I,'HxF_cal(2)' ]; bm_soil = bm_soil.drop(['HxF_cal(2)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_SF_3_1_1'])) & (~np.isnan(bm_soil['HxF_cal(3)' ]))); bm_soil.loc[I,'G_SF_3_1_1'] = bm_soil.loc[I,'HxF_cal(3)' ]; bm_soil = bm_soil.drop(['HxF_cal(3)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_SF_4_1_1'])) & (~np.isnan(bm_soil['HxF_cal(4)' ]))); bm_soil.loc[I,'G_SF_4_1_1'] = bm_soil.loc[I,'HxF_cal(4)' ]; bm_soil = bm_soil.drop(['HxF_cal(4)' ],axis=1)
                                                                                                                                                                                         
        I = ( (np.isnan(bm_soil['G_IU_1_1_1'])) & (~np.isnan(bm_soil['HxF_sig(1)' ]))); bm_soil.loc[I,'G_IU_1_1_1'] = bm_soil.loc[I,'HxF_sig(1)' ]; bm_soil = bm_soil.drop(['HxF_sig(1)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_IU_2_1_1'])) & (~np.isnan(bm_soil['HxF_sig(2)' ]))); bm_soil.loc[I,'G_IU_2_1_1'] = bm_soil.loc[I,'HxF_sig(2)' ]; bm_soil = bm_soil.drop(['HxF_sig(2)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_IU_3_1_1'])) & (~np.isnan(bm_soil['HxF_sig(3)' ]))); bm_soil.loc[I,'G_IU_3_1_1'] = bm_soil.loc[I,'HxF_sig(3)' ]; bm_soil = bm_soil.drop(['HxF_sig(3)' ],axis=1)
        I = ( (np.isnan(bm_soil['G_IU_4_1_1'])) & (~np.isnan(bm_soil['HxF_sig(4)' ]))); bm_soil.loc[I,'G_IU_4_1_1'] = bm_soil.loc[I,'HxF_sig(4)' ]; bm_soil = bm_soil.drop(['HxF_sig(4)' ],axis=1)
                                                                                                                                                                                         
        I = ( (np.isnan(bm_soil['TS_1_7_1'  ])) & (~np.isnan(bm_soil['Ts_1_7_1'   ]))); bm_soil.loc[I,'TS_1_7_1'  ] = bm_soil.loc[I,'Ts_1_7_1'   ]; bm_soil = bm_soil.drop(['Ts_1_7_1'   ],axis=1)
        I = ( (np.isnan(bm_soil['TS_2_7_1'  ])) & (~np.isnan(bm_soil['CS451_2(2)' ]))); bm_soil.loc[I,'TS_1_7_1'  ] = bm_soil.loc[I,'CS451_2(2)' ]; bm_soil = bm_soil.drop(['CS451_2(2)' ],axis=1)
                                                                                                                                                                                         
        I = ( (np.isnan(bm_soil['WCP_1_1_1' ])) & (~np.isnan(bm_soil['Press'      ]))); bm_soil.loc[I,'WCP_1_1_1' ] = bm_soil.loc[I,'Press'      ]; bm_soil = bm_soil.drop(['Press'      ],axis=1)
        I = ( (np.isnan(bm_soil['WCP_2_1_1' ])) & (~np.isnan(bm_soil['CS451_2(1)' ]))); bm_soil.loc[I,'WCP_2_1_1' ] = bm_soil.loc[I,'CS451_2(1)' ]; bm_soil = bm_soil.drop(['CS451_2(1)' ],axis=1)

        I = ( (np.isnan(bm_soil['SWC_1_1_1' ])) & (~np.isnan(bm_soil['CS655(1,1)' ]))); bm_soil.loc[I,'SWC_1_1_1' ] = bm_soil.loc[I,'CS655(1,1)' ]; bm_soil = bm_soil.drop(['CS655(1,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_1_2_1' ])) & (~np.isnan(bm_soil['CS655(2,1)' ]))); bm_soil.loc[I,'SWC_1_2_1' ] = bm_soil.loc[I,'CS655(2,1)' ]; bm_soil = bm_soil.drop(['CS655(2,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_1_3_1' ])) & (~np.isnan(bm_soil['CS655(3,1)' ]))); bm_soil.loc[I,'SWC_1_3_1' ] = bm_soil.loc[I,'CS655(3,1)' ]; bm_soil = bm_soil.drop(['CS655(3,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_1_4_1' ])) & (~np.isnan(bm_soil['CS655(4,1)' ]))); bm_soil.loc[I,'SWC_1_4_1' ] = bm_soil.loc[I,'CS655(4,1)' ]; bm_soil = bm_soil.drop(['CS655(4,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_1_5_1' ])) & (~np.isnan(bm_soil['CS655(5,1)' ]))); bm_soil.loc[I,'SWC_1_5_1' ] = bm_soil.loc[I,'CS655(5,1)' ]; bm_soil = bm_soil.drop(['CS655(5,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_3_1_1' ])) & (~np.isnan(bm_soil['CS655(6,1)' ]))); bm_soil.loc[I,'SWC_3_1_1' ] = bm_soil.loc[I,'CS655(6,1)' ]; bm_soil = bm_soil.drop(['CS655(6,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_2_1_1' ])) & (~np.isnan(bm_soil['CS655(7,1)' ]))); bm_soil.loc[I,'SWC_2_1_1' ] = bm_soil.loc[I,'CS655(7,1)' ]; bm_soil = bm_soil.drop(['CS655(7,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_2_2_1' ])) & (~np.isnan(bm_soil['CS655(8,1)' ]))); bm_soil.loc[I,'SWC_2_2_1' ] = bm_soil.loc[I,'CS655(8,1)' ]; bm_soil = bm_soil.drop(['CS655(8,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_2_3_1' ])) & (~np.isnan(bm_soil['CS655(9,1)' ]))); bm_soil.loc[I,'SWC_2_3_1' ] = bm_soil.loc[I,'CS655(9,1)' ]; bm_soil = bm_soil.drop(['CS655(9,1)' ],axis=1)
        I = ( (np.isnan(bm_soil['SWC_2_4_1' ])) & (~np.isnan(bm_soil['CS655(10,1)']))); bm_soil.loc[I,'SWC_2_4_1' ] = bm_soil.loc[I,'CS655(10,1)']; bm_soil = bm_soil.drop(['CS655(10,1)'],axis=1)
        I = ( (np.isnan(bm_soil['SWC_2_5_1' ])) & (~np.isnan(bm_soil['CS655(11,1)']))); bm_soil.loc[I,'SWC_2_5_1' ] = bm_soil.loc[I,'CS655(11,1)']; bm_soil = bm_soil.drop(['CS655(11,1)'],axis=1)
        I = ( (np.isnan(bm_soil['SWC_4_1_1' ])) & (~np.isnan(bm_soil['CS655(12,1)']))); bm_soil.loc[I,'SWC_4_1_1' ] = bm_soil.loc[I,'CS655(12,1)']; bm_soil = bm_soil.drop(['CS655(12,1)'],axis=1)


    # Before 21 Feb 2023, the column names were different. Rename.
    bm_soil = bm_soil.rename(columns = {'HxF_sig(1)' :   'G_IU_1_1_1',  'Press'      :    'WCP_1_1_1',
                                        'HxF_sig(2)' :   'G_IU_2_1_1',  'CS451_2(1)' :    'WCP_2_1_1',
                                        'HxF_sig(3)' :   'G_IU_3_1_1',    'Ts_1_7_1' :     'TS_1_7_1', 
                                        'HxF_sig(4)' :   'G_IU_4_1_1',  'CS451_2(2)' :     'TS_2_7_1',
                                        'HxF_cal(1)' :   'G_SF_1_1_1',
                                        'HxF_cal(2)' :   'G_SF_2_1_1',
                                        'HxF_cal(3)' :   'G_SF_3_1_1',
                                        'HxF_cal(4)' :   'G_SF_4_1_1',
                                        'CS655(1,1)' :    'SWC_1_1_1',  'CS655(1,4)' : 'SWC_IU_1_1_1',
                                        'CS655(2,1)' :    'SWC_1_2_1',  'CS655(2,4)' : 'SWC_IU_1_2_1',
                                        'CS655(3,1)' :    'SWC_1_3_1',  'CS655(3,4)' : 'SWC_IU_1_3_1',
                                        'CS655(4,1)' :    'SWC_1_4_1',  'CS655(4,4)' : 'SWC_IU_1_4_1',
                                        'CS655(5,1)' :    'SWC_1_5_1',  'CS655(5,4)' : 'SWC_IU_1_5_1',
                                        'CS655(6,1)' :    'SWC_3_1_1',  'CS655(6,4)' : 'SWC_IU_3_1_1',
                                        'CS655(7,1)' :    'SWC_2_1_1',  'CS655(7,4)' : 'SWC_IU_2_1_1',
                                        'CS655(8,1)' :    'SWC_2_2_1',  'CS655(8,4)' : 'SWC_IU_2_2_1',
                                        'CS655(9,1)' :    'SWC_2_3_1',  'CS655(9,4)' : 'SWC_IU_2_3_1',
                                        'CS655(10,1)':    'SWC_2_4_1',  'CS655(10,4)': 'SWC_IU_2_4_1',
                                        'CS655(11,1)':    'SWC_2_5_1',  'CS655(11,4)': 'SWC_IU_2_5_1',
                                        'CS655(12,1)':    'SWC_4_1_1',  'CS655(12,4)': 'SWC_IU_4_1_1'})
   
    if not 'G_1_1_1'in bm_soil.keys():  bm_soil['G_1_1_1'] = 0
    if not 'G_2_1_1'in bm_soil.keys():  bm_soil['G_2_1_1'] = 0
    if not 'G_3_1_1'in bm_soil.keys():  bm_soil['G_3_1_1'] = 0
    if not 'G_4_1_1'in bm_soil.keys():  bm_soil['G_4_1_1'] = 0
    
    I = ((bm_soil['G_SF_1_1_1'] == 0)); bm_soil.loc[I,'G_SF_1_1_1'] = 62.21
    I = ((bm_soil['G_SF_2_1_1'] == 0)); bm_soil.loc[I,'G_SF_2_1_1'] = 61.71
    I = ((bm_soil['G_SF_3_1_1'] == 0)); bm_soil.loc[I,'G_SF_3_1_1'] = 60.09
    I = ((bm_soil['G_SF_4_1_1'] == 0)); bm_soil.loc[I,'G_SF_4_1_1'] = 58.07
        
    I = ((bm_soil['G_1_1_1']    == 0)); bm_soil.loc[I,'G_1_1_1'   ] = 1000 * bm_soil.loc[I,'G_IU_1_1_1']/bm_soil.loc[I,'G_SF_1_1_1'] # 62.21
    I = ((bm_soil['G_2_1_1']    == 0)); bm_soil.loc[I,'G_2_1_1'   ] = 1000 * bm_soil.loc[I,'G_IU_2_1_1']/bm_soil.loc[I,'G_SF_2_1_1'] # 61.71
    I = ((bm_soil['G_3_1_1']    == 0)); bm_soil.loc[I,'G_3_1_1'   ] = 1000 * bm_soil.loc[I,'G_IU_3_1_1']/bm_soil.loc[I,'G_SF_3_1_1'] # 60.09
    I = ((bm_soil['G_4_1_1']    == 0)); bm_soil.loc[I,'G_4_1_1'   ] = 1000 * bm_soil.loc[I,'G_IU_4_1_1']/bm_soil.loc[I,'G_SF_4_1_1'] # 58.07
        
    I = ((bm_soil.index > pd.to_datetime('2023-02-21 09:50:00')) & (bm_soil.index < pd.to_datetime('2023-02-24 08:40:00')))
    T_tmp                     = bm_soil.loc[I,'TS_1_2_1'] # Store   TS_1_2_1 in T_tmp 
    bm_soil.loc[I,'TS_1_2_1'] = bm_soil.loc[I,'TS_1_1_1'] # Replace TS_1_2_1 by TS_1_1_1
    bm_soil.loc[I,'TS_1_1_1'] = T_tmp                     # Replace TS_1_1_1 by T_tmp (TS_1_2_1)
    T_tmp                     = bm_soil.loc[I,'TS_2_2_1'] # Store   TS_2_2_1 in T_tmp 
    bm_soil.loc[I,'TS_2_2_1'] = bm_soil.loc[I,'TS_2_1_1'] # Replace TS_2_2_1 by TS_1_1_1
    bm_soil.loc[I,'TS_2_1_1'] = T_tmp                     # Replace TS_2_1_1 by T_tmp (TS_2_2_1)
    
    if t2 <= np.datetime64('2023-03-02'):
        I = (bm_soil.index < pd.to_datetime('2023-02-21 16:30:00'))
        T_tmp                     = bm_soil.loc[I,'TS_4_1_1']
        bm_soil.loc[I,'TS_4_1_1'] = bm_soil.loc[I,'TS_4_2_1']
        bm_soil.loc[I,'TS_4_2_1'] = T_tmp
        
    
    # --- Water Table Depth
    #wtd_1_1_1          = -8.88 + 1000*bm_soil['Press']      / (rho_H2O * g) # kPa --> Pa --> m # West  plot. Gemeten 06 Feb 2023 12:24h. WCP = 31.46 kPa --> 3.18 m. Grondwaterstand was -5.70 m tov maaiveld. Dus de sensor was op -8.88 m
    #wtd_2_1_1          = -9.59 + 1000*bm_soil['CS451_2(1)'] / (rho_H2O * g) # kPa --> Pa --> m # North plot. Gemeten 15 Feb 2023 11:18h. WCP = 31.40 kPa --> 3.17 m. Grondwaterstand was -6.42 m tov maaiveld. Dus de sensor was op -9.59 m
    
    bm_soil            = bm_soil.astype(float)
    keys               = [key for key in bm_soil.keys() if 'TS_' in key]
    for key in keys:
        bm_soil[( (bm_soil[key] > 50) | (bm_soil[key] < -30))] = np.nan
    
    # Before 21 Feb 2021, Soil Water Content was measured in m3/m3, convert to % Volume
    I = (bm_soil.index < pd.to_datetime('2023-02-21 09:50:00'))
    keys = [key for key in bm_soil.keys() if (('SWC'   in key) & (not 'IU' in key))]
    for key in keys:
        bm_soil.loc[I,key] = bm_soil.loc[I,key]* 100 # Convert from m3/m3 to %(Volume)
    
    #ESG_SB_20231030+ Changed removing duplicates based on column name instead of column contents
    #bm_soil = bm_soil.T.drop_duplicates().T
    bm_soil = bm_soil.loc[:,~bm_soil.columns.duplicated()].copy()
    #ESG_SB_20231030-
        
    if not 'G_ISCAL_1_1_1' in bm_soil.keys(): bm_soil['G_ISCAL_1_1_1'] = 0
    if not 'G_ISCAL_2_1_1' in bm_soil.keys(): bm_soil['G_ISCAL_2_1_1'] = 0
    if not 'G_ISCAL_3_1_1' in bm_soil.keys(): bm_soil['G_ISCAL_3_1_1'] = 0
    if not 'G_ISCAL_4_1_1' in bm_soil.keys(): bm_soil['G_ISCAL_4_1_1'] = 0
    if not 'WTD_1_1_1'     in bm_soil.keys(): bm_soil['WTD_1_1_1'    ] = np.nan
    if not 'WTD_2_1_1'     in bm_soil.keys(): bm_soil['WTD_2_1_1'    ] = np.nan
    
    return bm_soil

###--- Loobos_Read_O3 NL-Loo_O3_yyyymmddhhmm_L06_F61.csv Fast Ozone Sensor
def Loobos_Read_NL_Loo_03(                t1,t2,keys=None,get_keys=False,datapath=None, API=False): 
    from Loobos_Toolbox_NewTower import getDatapath
    datapath             = getDatapath(None)
    all_keys             = ['TIMESTAMP','counter','O3_raw','multplier','0=press 1=temp 2=flow']
    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys

    if API == True:      # For the API we want one entire day file. The dayfiles start 20s after midnight and run until midnight.
        t_shift          = np.timedelta64(20,'s')
    else:
        t_shift          = 0
        
    t                    = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    nt                   = len(t)
    timeIndex            = pd.DatetimeIndex(t)
    O3                   = [] 
    for it in range(nt):
        tt               = t[it]
        yy               = tt.astype(object).year
        mm               = tt.astype(object).month
        dd               = tt.astype(object).day
        HH               = tt.astype(object).hour
        MM               = tt.astype(object).minute
        if ((dd == 1) & (HH == 0) & (MM == 0)):
            ttt          = t[it] - np.timedelta64(1,'D')
            yy2          = ttt.astype(object).year
            mm2          = ttt.astype(object).month
            filename         = os.path.join(datapath,'NL-Loo_O3','%4d'%yy2,'%02d'%mm2,'NL-Loo_O3_%4d%02d%02d%02d%02d_L06_F61.csv' %(yy,mm,dd,HH,MM))
        else:
            filename         = os.path.join(datapath,'NL-Loo_O3','%4d'%yy,'%02d'%mm,'NL-Loo_O3_%4d%02d%02d%02d%02d_L06_F61.csv' %(yy,mm,dd,HH,MM))
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:
            print('File not found: ',filename)
            file_success = False
        if file_success:
            O3_tmp       = pd.read_csv(os.path.join(datapath,filename),sep=',',parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S.%f',index_col='TIMESTAMP',usecols=keys)
#           O3_tmp       = pd.read_csv(os.path.join(datapath,filename),sep=',',                                                        index_col='TIMESTAMP',usecols=keys)
#           O3_tmp.index = pd.to_datetime(O3_tmp.index,format='%Y%m%d%H%M%S.%2f')
            O3.append(O3_tmp)
    if len(O3) > 0:
        O3          =  pd.concat(O3, axis=0,ignore_index=False)
        
    else:
        O3 = pd.DataFrame(index=timeIndex, columns=keys)
    
    O3.index      = O3.index.round('50ms')
    return O3
    
###--- Loobos_Read_O3 NL-Loo_O3_flux: mean concentration Loobos, Wekerom and O3 flux from intermediate file NL-Loo_O3_yyyymmdd_L06_F62.csv
def Loobos_Read_NL_Loo_03_flux( t1,t2,keys=None,get_keys=False,datapath=None, API=False):
    from Loobos_Toolbox_NewTower import getDatapath
    datapath             = getDatapath(None)
    all_keys             = ['TIMESTAMP','C_O3','F_O3','tau','F_O3_1s']

    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys

    if API == True:      # For the API we want one entire day file. The dayfiles start 20s after midnight and run until midnight.
        t_shift          = np.timedelta64(20,'s')
    else:
        t_shift          = 0
        
    t                    = np.arange(t1,t2,np.timedelta64(1,'D')).astype(np.datetime64)
    nt                   = len(t)
    timeIndex            = pd.DatetimeIndex(t)
    O3                   = list()
    aq                   = list()
    for it in range(nt):
        tt               = t[it]
        yy               = tt.astype(object).year
        mm               = tt.astype(object).month
        dd               = tt.astype(object).day
        
        # Load NL-Loo_O3 flux files (F62)
        filename         = os.path.join(datapath,'NL-Loo_O3','%4d'%yy,'%02d'%mm,'NL-Loo_O3_%4d%02d%02d_L06_F62.csv' %(yy,mm,dd))
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:
            print('File not found: ',filename)
            file_success = False
        if file_success:
            O3.append(pd.read_csv(os.path.join(datapath,filename),sep=',',parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S',index_col='TIMESTAMP',usecols=keys))
            
        # Load RIVM Wekerom files
        aq_keys = ['TIMESTAMP','time','year','month','day','hour','NO','NO2','O3','PM10','PM25','FN','PS','NH3','CO','C6H6','C7H8','SO2','H2S']
        filename         = os.path.join(datapath,'NL-Loo_O3','%4d'%yy,'%02d'%mm,'Wekerom_%4d%02d%02d.csv' %(yy,mm,dd))
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:
            print('File not found: ',filename)
            file_success = False
        if file_success:
            aq.append(pd.read_csv(os.path.join(datapath,filename),sep=',',parse_dates=['TIMESTAMP'],date_format='%Y-%m-%d %H:%M:%S',index_col='TIMESTAMP',usecols=aq_keys))
            
    if len(O3) > 0:
        O3          =  pd.concat(O3, axis=0,ignore_index=False)
    else:
        O3 = pd.DataFrame(index=timeIndex, columns=keys)
    if len(aq) > 0:
        aq          =  pd.concat(aq, axis=0,ignore_index=False)
    else:
        aq = pd.DataFrame(index=timeIndex, columns=aq_keys)
        
    return O3,aq

###--- Loobos_Read_O3 NL-Loo_O3_abs: Concentrations from Thermo 49C on 1 minute basis
def Loobos_Read_NL_Loo_O3_abs(            t1,t2,keys=None,get_keys=False,datapath=None,API=False):
    from Loobos_Toolbox_NewTower import getDatapath
    datapath             = getDatapath(None)
    all_keys             = ["TIMESTAMP","O3","flags","intA","intB","btemp","ltmp","flow A","flow B","pressHg","press"]

    if keys is None:
        keys             = all_keys
    if get_keys == True:
        return all_keys
    if API == True:      # For the API we want one entire day file. The dayfiles start 1 minute after midnight and run until midnight.
        t_shift          = np.timedelta64(1,'m')
    else:
        t_shift          = 0
 
#   t                    = np.arange(t1+t_shift,t2+t_shift,np.timedelta64(1,'m')).astype(np.datetime64)
    t                    = np.arange(t1+t_shift,t2+np.timedelta64(1,'D')-np.timedelta64(1,'s')+t_shift,np.timedelta64(1,'m')).astype(np.datetime64)
    d1                   = np.datetime64('%4d-%02d-%02d'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    d2                   = np.datetime64('%4d-%02d-%02d'%(t2.astype(object).year,t2.astype(object).month,t2.astype(object).day))
    days                 = np.arange(d1,d2+np.timedelta64(1,'D'),np.timedelta64(1,'D')).astype(np.datetime64)
    ndays                = len(days)
    timeIndex            = pd.DatetimeIndex(t)
    O3_abs               = [] 
    for iday in range(ndays):
        day              = days[iday]
        yy               = day.astype(object).year
        mm               = day.astype(object).month
        dd               = day.astype(object).day
        filename         = os.path.join(datapath,'NL-Loo_O3_abs','%4d'%yy,'%02d'%mm,'NL-Loo_O3_%4d%02d%02d_L07_F71.csv' %(yy,mm,dd))
        if os.path.exists(filename):
            print('File found: ',os.path.exists(filename),filename)
            file_success = True
        else:
            print('File not found: ',filename)
            file_success = False
        if file_success:
           #O3_abs.append(pd.read_csv(os.path.join(datapath,filename),sep=',',parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S.%f',index_col='TIMESTAMP',usecols=keys))
#           O3_abs.append(pd.read_csv(os.path.join(datapath,filename),sep=',',names=keys,header=0,index_col='TIMESTAMP',parse_dates=['TIMESTAMP'],date_format='%Y%m%d%H%M%S.%f'))
            O3_abs.append(pd.read_csv(os.path.join(datapath,filename),sep=',',skiprows=(0,2,3),parse_dates=['TIMESTAMP'],index_col='TIMESTAMP',date_format='%Y%m%d%H%M%S.%f'))

    if len(O3_abs) > 0:
        O3_abs           =  pd.concat(O3_abs, axis=0,ignore_index=False)
    else:
        O3_abs           = pd.DataFrame(index=timeIndex, columns=keys)
    O3_abs.index         = O3_abs.index.round('60s')
    O3_abs               = O3_abs[~O3_abs.index.duplicated()]
    O3_abs               = O3_abs.reindex(timeIndex)
    return O3_abs
    
#================================================================================================
def Loobos_ReadVOC(t1, t2, keys=None,crop=False):
    import glob
    datapath   = getDatapath(None)
    names      = ['TIMESTAMP', '14.004', '15.024', '15.996', '17.028', '18.035', '19.023', '19.201', '20.023', '21.022', '22.989', '26.014', '28.004', '29.012', '29.996', '30.993', '31.016', '31.988', '32.992', '33.031', '33.991', '34.993', '35.035', '36.018', '37.027', '38.031', '39.029', '39.958', '41.036', '42.005', '42.030', '43.014', '43.051', '43.987', '44.011', '44.052', '44.976', '44.993', '45.030', '45.989', '46.026', '47.009', '47.046', '48.005', '48.049', '49.009', '49.999', '51.003', '51.022', '51.043', '51.939', '51.994', '52.029', '52.940', '53.002', '53.038', '53.938', '53.992', '54.034', '55.039', '55.934', '56.045', '57.038', '57.069', '57.934', '58.032', '58.071', '59.049', '59.930', '60.049', '60.079', '61.028', '61.925', '61.976', '62.028', '62.993', '63.002', '63.026', '63.042', '63.978', '64.000', '64.032', '65.023', '65.059', '66.017', '66.046', '66.499', '67.053', '67.934', '68.019', '68.054', '68.996', '69.033', '69.069', '70.032', '70.070', '71.012', '71.048', '71.085', '71.931', '72.013', '72.050', '72.088', '72.936', '73.029', '73.062', '74.022', '74.063', '75.043', '75.945', '76.042', '77.022', '77.059', '77.940', '78.046', '79.042', '79.937', '80.040', '81.035', '81.069', '82.072', '82.943', '82.988', '83.012', '83.048', '83.085', '84.046', '84.085', '84.964', '85.028', '85.064', '85.100', '86.025', '86.062', '87.043', '87.079', '88.041', '88.077', '88.952', '89.024', '89.059', '90.018', '90.061', '90.947', '91.044', '91.945', '92.054', '93.037', '93.068', '93.955', '94.039', '94.073', '95.021', '95.046', '95.085', '95.951', '96.019', '96.049', '96.088', '96.960', '97.027', '97.063', '97.100', '97.949', '98.027', '98.064', '98.103', '98.956', '99.008', '99.043', '99.079', '99.949', '100.040', '100.080', '100.937', '101.023', '101.059', '101.069', '101.094', '102.021', '102.060', '102.091', '102.934', '103.039', '103.073', '103.949', '104.045', '104.929', '105.025', '105.065', '105.934', '106.073', '106.963', '107.084', '107.957', '108.041', '108.089', '108.958', '109.031', '109.060', '109.100', '109.956', '110.035', '110.062', '110.104', '110.960', '111.047', '111.115', '111.964', '112.046', '112.955', '113.029', '113.055', '113.094', '113.961', '114.022', '114.056', '115.009', '115.037', '115.074', '116.035', '116.076', '116.906', '117.018', '117.089', '117.956', '118.025', '118.050', '118.903', '119.034', '119.075', '119.950', '120.037', '120.900', '120.958', '121.030', '121.098', '121.954', '122.058', '122.103', '122.897', '122.960', '123.042', '123.116', '123.947', '124.043', '124.119', '124.972', '125.024', '125.095', '125.130', '125.959', '126.969', '127.075', '127.948', '128.041', '128.075', '128.969', '129.062', '129.088', '129.126', '129.944', '130.053', '130.966', '131.038', '131.103', '131.941', '132.029', '133.025', '133.098', '134.103', '134.958', '135.032', '135.114', '135.955', '136.031', '136.119', '136.954', '137.132', '137.961', '138.135', '138.964', '139.039', '139.111', '139.965', '140.040', '140.114', '140.967', '141.024', '141.090', '141.956', '142.964', '143.041', '143.086', '143.098', '143.970', '144.038', '144.980', '145.051', '145.118', '145.958', '146.048', '146.978', '147.055', '147.113', '148.036', '149.027', '149.103', '150.035', '150.101', '151.037', '151.110', '152.038', '152.116', '153.058', '153.124', '154.021', '154.049', '154.127', '154.964', '155.103', '155.972', '156.034', '156.099', '157.052', '157.111', '157.987', '159.038', '159.132', '159.966', '161.049', '161.130', '162.043', '162.971', '163.043', '163.132', '164.022', '164.970', '165.035', '165.087', '165.159', '166.029', '166.093', '166.500', '167.054', '167.103', '168.053', '168.109', '169.038', '169.118', '170.036', '170.122', '171.041', '171.080', '171.109', '171.132', '172.840', '172.956', '173.083', '174.954', '175.059', '176.050', '176.924', '177.038', '177.055', '177.148', '178.924', '179.049', '179.082', '179.174', '179.919', '180.919', '181.066', '181.102', '181.117', '181.917', '182.026', '182.918', '183.037', '183.099', '184.024', '184.105', '184.920', '185.076', '185.114', '187.054', '187.091', '189.071', '189.981', '190.966', '191.164', '191.976', '192.965', '193.188', '195.023', '197.064', '197.077', '198.111', '199.079', '199.176', '200.087', '201.090', '202.033', '203.176', '203.942', '204.947', '205.950', '206.198', '207.177', '208.975', '211.199', '213.205', '215.070', '217.065', '217.945', '218.949', '219.047', '219.183', '219.955', '220.946', '221.185', '223.063', '224.062', '225.044', '225.217', '226.043', '227.034', '227.216', '230.995', '232.989', '235.956', '236.957', '239.236', '241.073', '241.221', '242.039', '243.052', '245.036', '247.007', '247.226', '255.233', '256.783', '257.023', '257.236', '258.011', '259.020', '260.025', '261.241', '266.499', '275.256', '289.264', '297.081', '298.073', '299.062', '300.059', '301.049', '329.837', '330.845', '331.848', '332.851', '345.851', '346.838', '347.868', '348.856', '355.062', '366.499', '371.098', '372.091', '373.079', '374.077', '375.063', '429.079', '445.110', '447.095', '466.598', '566.599', '666.598', '766.599']
    t          = pd.date_range(t1,t2,freq='1800s',inclusive='left')
    nt         = len(t)
    dataloaded = list()
    VOC        = list()
    for it in range(nt):
        tt                     = t[it]
    
        #--- 1. Load VOC file(s) -----------------------------------------------------------------------------
        tt1                    = tt - pd.Timedelta('15m') # file starting 15 m before start interval 
        tt2                    = tt + pd.Timedelta('15m') # file starting 15 m after  start interval 
        filename_search1       = os.path.join(datapath,'NL-Loo_VOC','%4d'%tt1.year,'%02d'%tt1.month,'ppb%4d.%02d.%02d-%02dh%1d*.csv'%(tt1.year,tt1.month,tt1.day,(tt1-pd.Timedelta('1h')).hour,np.floor(tt1.minute/10)))
        filename_search2       = os.path.join(datapath,'NL-Loo_VOC','%4d'%tt2.year,'%02d'%tt2.month,'ppb%4d.%02d.%02d-%02dh%1d*.csv'%(tt2.year,tt2.month,tt2.day,(tt2-pd.Timedelta('1h')).hour,np.floor(tt2.minute/10)))
        print('Looking for %s and %s'%(filename_search1,filename_search2))
        filenames              = list()
        if not filename_search1 in dataloaded:
            filename1          = glob.glob(filename_search1);
            print('Found filenames',filename1)
            dataloaded         .append(filename_search1)
            if len(filename1)>0:   filenames.append(filename1[0])
        else: 
            print('VOC file already loaded: ',filename_search1)
        if not filename_search2 in dataloaded:
            filename2          = glob.glob(filename_search2); 
            print('Found filenames',filename2)
            dataloaded         .append(filename_search2)
            if len(filename2)>0:   filenames.append(filename2[0])
        else: 
            print('VOC file already loaded: ',filename_search2)
    
        # Loading VOC file starting 15 m before start interval and ending 15 m after interval
        for filename in filenames:
            print('Loading           %s'%filename)
            VOC_tmp            = pd.read_csv(filename,header=0,skiprows=[0],names=names,usecols=['TIMESTAMP','33.031','59.049','69.069','71.048','137.132'],index_col='TIMESTAMP')
            VOC_tmp.index      = (pd.to_datetime(VOC_tmp.index,unit='D',origin='2009-01-01') + pd.Timedelta('01:00:00')) # UTC --> CET
            VOC_tmp.index      = VOC_tmp.index.round('500ms')
            VOC.append(VOC_tmp)
    VOC                        = pd.concat(VOC,axis=0,ignore_index=False)
    VOC                        = VOC[~VOC.index.duplicated()]
    
    if crop == True:
        timeIndex              = pd.DatetimeIndex(pd.date_range(t1,t2,freq='500ms',inclusive='left'))
        VOC                    = VOC.reindex(timeIndex)
    return VOC