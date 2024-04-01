#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pdfAuto




def main():
    
    ts = datetime.datetime.now()
    month = ts.month
    year = ts.year
    
    month = 3
    #year = 2023
    pdfpath = '/home/azureuser/deddhePDF'
    
    address = 'http://localhost:8080'
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    entityId = '4795fc10-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    #assetid = '623708c0-7317-11ee-b97f-098b03a95eb1' # DEDDHE Athinas 0.all devices
    
    for i in range(0,len(r1['data'])):
     #   os.chdir('/home/azureuser/deddhePDF/')
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        #print(assetname)
    
        #if not os.path.exists(assetname):
        #    os.makedirs(assetname)
        
        r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
        #if assetid=='fcb94bd0-8df5-11ee-8694-75852126ee2f':
        #    print('found device')
        #    device = '102.408.000200'
        #    pdfAuto.main(device, month, year, assetname, pdfpath)
        for j in range(0, len(r2)):
            device = r2[j]['toName']
            if device[:3]=='102':
                print(device)
                # call pdf export function
                #try:
                pdfAuto.main(device, month, year, assetname, pdfpath)
                #except:
                #    pass
                
                # get device info
                [devid, _, _, _, _, _] = pdfAuto.get_dev_info(device, address)
                
                # search for nested devices
                r3 = requests.get(url=address + "/api/relations/info?fromId="+devid+"&fromType=DEVICE",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
                for k in range(0, len(r3)):
                  device = r3[k]['toName']
                  print(device)
                  # call pdf export function
                  #try:
                  pdfAuto.main(device, month, year, assetname, pdfpath)
                  #except:
                  #    pass
            
            
            
            

if __name__ == '__main__':
    sys.exit(main())