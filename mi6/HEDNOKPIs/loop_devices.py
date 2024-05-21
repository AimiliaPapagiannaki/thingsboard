#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pandas as pd
import exportKPIs



def main():
    
    start_time = '1709244000000' # March 1
    end_time = '1711918800000' # April 1
    #start_time = '1701381600000' # Dec 1st
    #end_time = '1708466400000' # Feb 21st
    
    address = 'http://localhost:8080'
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    entityId = '4795fc10-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    summary = pd.DataFrame(columns=['Installation','Transformer','Distribution','Total energy consumption (MWh)','Nr. of power alarms L1','Min duration of power alarms L1 (min)','Max duration of power alarms L1 (min)','Nr. of power alarms L2','Min duration of power alarms L2 (min)','Max duration of power alarms L2 (min)','Nr. of power alarms L3','Min duration of power alarms L3 (min)','Max duration of power alarms L3 (min)','Nr. of overcurrent alarms L1','Min duration of overcurrent alarms L1 (min)','Max duration of overcurrent alarms L1 (min)','Nr. of overcurrent alarms L2','Min duration of overcurrent alarms L2 (min)','Max duration of overcurrent alarms L2 (min)','Nr. of overcurrent alarms L3','Min duration of overcurrent alarms L3 (min)','Max duration of overcurrent alarms L3 (min)','Nr. of Voltage unbalance alarms','% of time of Voltage unbalance alarms','Nr. of Current unbalance alarms','% of time of Current unbalance alarms','Nr. of Power Fails (outage)','Min time of Power Fails (sec)','Max time of Power Fails (sec)','Nr. of Voltage dips','Avg time of Voltage dips (msec)','Nr. of Voltage swells','Avg time of Voltage swells (msec)'])
    
    
  
    
    for i in range(0,len(r1['data'])):
     #   os.chdir('/home/azureuser/deddhePDF/')
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        print(assetname)
    
        if assetname[0]!='0':
        #if assetname=='ΜΠ-172 Κ/Δ ΠΑΛΛΗΝΗΣ':
        
            r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            
            
            for j in range(0, len(r2)):
                device = r2[j]['toName']
                if device[:3]=='102':
                    print(device)
                    isdist = False
                    kpis = {}
                    kpis['Installation'] = assetname
                    
                    
                    
                    # call export KPIs function
                    #try:
                    [devid, acc_token, label] = exportKPIs.get_dev_info(device, address)
                    kpis['Transformer'] = label
                    kpis['Distribution'] = ''
                    
                    
                    kpis = exportKPIs.main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist)
                    
                    # get lat lon
                    [lat,lon] = exportKPIs.get_lat_lon(devid, acc_token, address)
                    kpis['Latitude'] = lat
                    kpis['Longitude'] = lon
                    summary = summary.append(kpis, ignore_index=True)
                   
                    #except:
                    #    pass
                    
                    
                    
                    
                    # search for nested devices
                    r3 = requests.get(url=address + "/api/relations/info?fromId="+devid+"&fromType=DEVICE",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
                    for k in range(0, len(r3)):
                      kpis = {}
                      kpis['Installation'] = assetname
                      isdist = True
                      
                      device = r3[k]['toName']
                      print(device)
                      kpis['Transformer'] = label
                      kpis['Distribution'] = device
                      
                      [devid, acc_token, _] = exportKPIs.get_dev_info(device, address)
                      
                      # call pdf export function
                      #try:
                      kpis = exportKPIs.main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist)
                      kpis['Latitude'] = lat
                      kpis['Longitude'] = lon
                      summary = summary.append(kpis, ignore_index=True)
                      
                      
                      
                      #except:
                          #pass
                
        
    summary.to_excel('HEDNO_KPIs_3_2024.xlsx', index=False)
    #summary.to_excel('testPwrAlarms.xlsx', index=False)        
            
            

if __name__ == '__main__':
    sys.exit(main())