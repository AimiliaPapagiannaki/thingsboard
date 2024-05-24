#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pandas as pd
import convolKPI



def main():


    startTs = {}
    startTs['Jan'] = '1704060000000'
    startTs['Feb'] = '1706738400000'
    startTs['Mar'] = '1709244000000'
    startTs['Apr'] = '1711918800000'
    startTs['May'] = '1714510800000'
    
    endTs = {}
    endTs['Jan'] = '1706738400000'
    endTs['Feb'] = '1709244000000'
    endTs['Mar'] = '1711918800000'
    endTs['Apr'] = '1714510800000'
    endTs['May'] = '1716325200000'
    
    for name in ['Jan','Feb','Mar','Apr','May']:
    
        start_time = startTs[name] 
        end_time = endTs[name]
        
        address = 'http://localhost:8080'
        r = requests.post(address + "/api/auth/login",
                          json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
        
        acc_token = 'Bearer' + ' ' + r['token']
        
        
        entityId = '4795fc10-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
        r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
    'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
        summary = pd.DataFrame(columns=['Installation','Transformer','Nr. of power alarms L1','Avg duration of power alarms L1 (min)','Avg % of exceeding threshold L1 (min)','Nr. of power alarms L2','Avg duration of power alarms L2 (min)','Avg % of exceeding threshold L2 (min)','Nr. of power alarms L3','Avg duration of power alarms L3 (min)','Avg % of exceeding threshold L3 (min)'])
        
        
        
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
         
                        # call convolKPI function
                        #try:
                        [devid, acc_token, label] = convolKPI.get_dev_info(device, address)
                        kpis['Transformer'] = label                 
                        
                        kpis = convolKPI.main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist)
                        
                       
                        #summary = summary.append(kpis, ignore_index=True)
                        summary = pd.concat([summary, pd.DataFrame([kpis])], ignore_index=True)
    
    
            
        summary.to_excel('HEDNO_overpwr_'+name+'_2024.xlsx', index=False)  
            
            

if __name__ == '__main__':
    sys.exit(main())