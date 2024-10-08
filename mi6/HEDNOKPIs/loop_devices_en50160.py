#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pandas as pd
import exportKPIs_en50160
from dateutil.relativedelta import relativedelta
import pytz


def main():
    
    monthdict = {1:'Jan', 2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
  
    
    address = 'http://localhost:8080'
    for month in [9]:
        startm = datetime.datetime(year = 2024, month=month, day=1)
        endm = startm + relativedelta(months=1)
        tmzn = pytz.timezone('Europe/Athens')    
        endm = tmzn.localize(endm)
        startm = tmzn.localize(startm)
        end_time = str(int((endm ).timestamp() * 1000))
        start_time = str(int((startm ).timestamp() * 1000))
    #for name in ['Jun']:
    #    start_time = startTs[name] 
    #    end_time = endTs[name]
    
    
        r = requests.post(address + "/api/auth/login",
                          json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
        
        acc_token = 'Bearer' + ' ' + r['token']
        
        
        entityId = '47545f30-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
        r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
    'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
        summary = pd.DataFrame(columns=['Installation','Transformer','Distribution','Max voltage deviation (95% of 10min intervals)','Max positive voltage deviation (100% of 10min intervals)','Max negative voltage deviation (100% of 10min intervals)','Max voltage imbalance (95% of 10min intervals)','Max voltage THD (95% of 10min intervals)','Max current THD L1','Max current THD L2','Max current THD L3','Max frequency deviation (99.5% of 10min intervals)','Max positive frequency deviation (100% of 10min intervals)','Max negative frequency deviation (100% of 10min intervals)','Nr. of Power Fails (outage)','Nr. of Voltage dips','Nr. of Voltage swells','Occurences of Frequency over limit','Occurences of Frequency under limit','Nr. of RVCs'])
        
        
      
        
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
                        [devid, acc_token, label] = exportKPIs_en50160.get_dev_info(device, address)
                        kpis['Transformer'] = label
                        kpis['Distribution'] = ''
                        
                        
                        kpis = exportKPIs_en50160.main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist)
                        
                        # get lat lon
                        [lat,lon] = exportKPIs_en50160.get_lat_lon(devid, acc_token, address)
                        kpis['Latitude'] = lat
                        kpis['Longitude'] = lon
                        #summary = summary.append(kpis, ignore_index=True)
                        
                        summary = pd.concat([summary, pd.DataFrame([kpis])], ignore_index=True)
                        #summary = pd.concat([summary, pd.DataFrame.from_dict(kpis, orient='columns')], axis=0)
                        
                        #print(summary)
                        #except:
                        #    pass
                        
                        
                        
                        
                        # search for nested devices
                        #r3 = requests.get(url=address + "/api/relations/info?fromId="+devid+"&fromType=DEVICE",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
                        #for k in range(0, len(r3)):
                        #  kpis = {}
                        #  kpis['Installation'] = assetname
                        #  isdist = True
                        #  
                        #  device = r3[k]['toName']
                        #  print(device)
                        #  kpis['Transformer'] = label
                        #  kpis['Distribution'] = device
                        #  
                        #  [devid, acc_token, _] = exportKPIs_en50160.get_dev_info(device, address)
                        #  
                          # call pdf export function
                          #try:
                        #  kpis = exportKPIs_en50160.main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist)
                        #  kpis['Latitude'] = lat
                        #  kpis['Longitude'] = lon
                        #  #summary = summary.append(kpis, ignore_index=True)
                        #  summary = pd.concat([summary, pd.DataFrame([kpis])], ignore_index=True)
                          
                          
                          #except:
                              #pass
                    
            
        summary.to_excel('HEDNO_KPIs_EN50160_'+monthdict[month]+'_2024.xlsx', index=False)
    #summary.to_excel('testPwrAlarms.xlsx', index=False)        
            
            

if __name__ == '__main__':
    sys.exit(main())