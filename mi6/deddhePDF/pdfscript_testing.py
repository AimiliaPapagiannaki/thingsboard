#!/usr/bin/env python3 

import sys
import pandas as pd
import numpy as np
import requests
import os
import glob
import pytz
import matplotlib.dates as mdates
import json
import math
import ast
from sys import stdout as out
from fpdf import FPDF
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 14})
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import warnings
warnings.filterwarnings("ignore")

class FPDF(FPDF):
    # Page footer
    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, str(self.page_no()), 0, 0, 'C')
    def header(self):
        self.set_y(10)
        # self.image('/home/azureuser/deddhePDF/logos/meazon.png', x=10, y=10, w=30, h=10)
        # self.image('/home/azureuser/deddhePDF/logos/deddie.jpg', x=250, y=10, w=30, h=10)
        self.image('E:/VSprojects/thingsboard/mi6/deddhePDF/logos/meazon.png', x=10, y=10, w=30, h=10)
        self.image('E:/VSprojects/thingsboard/mi6/deddhePDF/logos/deddie.jpg', x=250, y=10, w=30, h=10)



def create_pdf(filename, month_Name, month_Name2, year, year2,day1,day2,device,frqflag):

    # try:        
    pdf = FPDF()

    # page 1
    # orientation & greek fonts
    pdf.add_page(orientation='L')
    # pdf.add_font('DejaVu', '', r"/usr/local/lib/python3.8/dist-packages/fpdf/fonts/DejaVuSansCondensed.ttf", uni=True)
    # pdf.add_font('DejaVuB', '', r"/usr/local/lib/python3.8/dist-packages/fpdf/fonts/DejaVuSans-Bold.ttf", uni=True)
    pdf.add_font('DejaVu', '', r"E:\Python311\Lib\site-packages\fpdf\fonts\DejaVuSansCondensed.ttf", uni=True)
    pdf.add_font('DejaVuB', '', r"E:\Python311\Lib\site-packages\fpdf\fonts\DejaVuSans-Bold.ttf", uni=True)
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20)
    # pdf.set_font('arial', 'B', 14)
    # pdf.cell(0,8, "Moxy Patras", 0, 1, 'C')
    pdf.cell(0, 10, "Ανάλυση ποιότητας τάσης "+str(day1)+"/"+str(month_Name)+"/"+str(year)+" - "+str(day2)+"/"+ str(month_Name2)+"/"+str(year2), 0, 1, 'C')
    pdf.set_font('DejaVu', '', 12)
    pdf.cell(0,10,"Σειριακός αριθμός μετρητή:"+str(device), 0, 1, 'C')

    pdf.image('metrics.png', x=50, y=50, w=210, h=80, type='', link='')
    pdf.set_xy(20, 170)
    pdf.cell(0,10,"*Περιγραφή υπολογισμών στο Παράρτημα Α' & Β'", 0, 1, 'L')

    #page 2
    pdf.add_page(orientation='L')
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20) 
    pdf.cell(0, 10, "Βυθίσεις/Υπερτάσεις (Dips/Swells) ", 0, 1, 'C')
    pdf.set_font('DejaVu', '', 10)

    pdf.image('dipsL1.png',x=10, y=30, w=150, h=50, type='', link='')
    pdf.image('swellsL1.png',x=170, y=30, w=90, h=50, type='', link='')

    pdf.image('dipsL2.png',x=10, y=80, w=150, h=50, type='', link='')
    pdf.image('swellsL2.png',x=170, y=80, w=90, h=50, type='', link='')

    pdf.image('dipsL3.png',x=10, y=130, w=150, h=50, type='', link='')
    pdf.image('swellsL3.png',x=170, y=130, w=90, h=50, type='', link='')

    pdf.set_xy(20, 175)
    pdf.cell(0,10,"Ως ελάχιστη/μέγιστη τάση u% νοείται η ποσοστιαία τιμή της ακραίας τάσης σε σχέση με την τάση αναφοράς (230V), δηλαδή u/230 *100", 0, 1, 'L')


    #page 2
    pdf.add_page(orientation='L')
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20)
    pdf.cell(0, 10, "Τάση - Απόκλιση Τάσης ", 0, 1, 'C')
    pdf.set_font('DejaVu', '', 12)
    pdf.cell(0,10,"Συνολική τάση: Average(L1,L2,L3)", 0, 1, 'L')
    pdf.image('totalVlt.png',x=5, w=280, h=65, type='', link='')

    pdf.cell(0,10,"Ποσοστό κατ'απόλυτο απόκλισης τάσης από nominal (230V)", 0, 1, 'L')
    pdf.image('deviation.png',x=5, w=280, h=65, type='', link='')

    #page 3
    if frqflag==1:
        pdf.add_page(orientation='L')
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        pdf.cell(0, 10, "Συχνότητα - Απόκλιση Συχνότητας ", 0, 1, 'C')
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0,10,"Συχνότητα Average(L1,L2,L3)", 0, 1, 'L')
        pdf.image('efrq.png',x=5, w=280, h=65, type='', link='')

        pdf.cell(0,10,"Ποσοστό κατ'απόλυτο απόκλισης συχνότητας από nominal (50Hz)", 0, 1, 'L')
        pdf.image('frqdev.png',x=5, w=280, h=65, type='', link='')

    #page 4
    pdf.add_page(orientation='L')
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20)
    pdf.cell(0, 10, "Ασυμμετρία τάσης - Αρμονική παραμόρφωση ", 0, 1, 'C')
    pdf.set_font('DejaVu', '', 12)
    pdf.cell(0,10,"Ασυμμετρία τάσης %", 0, 1, 'L')
    pdf.image('imbalance.png',x=5, w=280, h=65, type='', link='')

    pdf.cell(0,10,"Αρμονική παραμόρφωση τάσης %", 0, 1, 'L')
    pdf.image('totalVthd.png',x=5, w=280, h=65, type='', link='')

    #page 5
    pdf.add_page(orientation='L')
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20)
    pdf.cell(0, 10, "Παράρτημα Α'", 0, 1, 'C')

    pdf.set_font('DejaVuB', '', 12)
    pdf.cell(0, 7.5, "Περιγραφή υπολογισμού χαρακτηριστικών τάσης", 0, 1, 'C')
    pdf.multi_cell(0, 7.5, "Μέγιστη απόκλιση τάσης", 0, 1, 'L')
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 7.5, "Για κάθε τιμή της ανά 10λεπτο τάσης, υπολογίζεται η κατ'απόλυτο ποσοστιαία διαφορά από τα 230V --> |(x-230)/230|", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Για το 100% του χρόνου, υπολογίζεται το max των παραπάνω αποκλίσεων.", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Για το 95% του χρόνου, υπολογίζεται το 95o percentile των αποκλίσεων (Διάταξη λίστας τιμών σε αύξουσα σειρά και επιλογή του στοιχείου που ο index του αντιστοιχεί στο 95% του μήκους της λίστας στοιχείων)", 0, 1, 'L')
    
    pdf.set_font('DejaVuB', '', 12)
    pdf.multi_cell(0, 7.5, "Μέγιστη ασυμμετρία τάσης", 0, 1, 'L')
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 7.5, "Υπολογίζεται ο μέσος όρος των 3 φάσεων και στη συνέχεια η απόλυτη διαφορά κάθε φάσης από το μέσο όρο. Η ασυμμετρία προκύπτει από τη διαίρεση της μέγιστης διαφοράς με το μέσο όρο. (Η μέγιστη ασυμμετρία για το 95% του χρόνου προκύπτει όπως και προηγουμένως)", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Mέση τάση -->  mean_val = (vltA + vltB + vltC)/3", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "απόκλιση φάσης Α από τη μέση τάση --> dif1 = |(vltA - mean_val)|", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "απόκλιση φάσης Β από τη μέση τάση -->  dif2 = |(vltB - mean_val)|", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "απόκλιση φάσης C από τη μέση τάση -->  dif3 = |(vltC - mean_val)|", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Ασυμμετρία τάσης --> volt_imb = (max(dif1, dif2, dif3) / mean_val) * 100", 0, 1, 'L')

    pdf.set_font('DejaVuB', '', 12)
    pdf.multi_cell(0, 7.5, "Μέγιστη αρμονική παραμόρφωση", 0, 1, 'L')
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 7.5, "Υπολογίζεται η ποσοστιαία αρμονική παραμόρφωση ανά 10λεπτο, και η μέγιστη για το 95% του χρόνου προκύπτει όπως και προηγουμένως.", 0, 1, 'L')

    pdf.set_font('DejaVuB', '', 12)
    pdf.multi_cell(0, 7.5, "Μέγιστη απόκλιση συχνότητας", 0, 1, 'L')
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 7.5, "Για κάθε τιμή της ανά 10λεπτο συχνότητας, υπολογίζεται η κατ'απόλυτο ποσοστιαία διαφορά από τα 50Hz--> |(x-50)/50| ", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Για το 100% του χρόνου, υπολογίζεται το max των παραπάνω αποκλίσεων.", 0, 1, 'L')
    pdf.multi_cell(0, 7.5, "Για το 95% του χρόνου, υπολογίζεται το 95o percentile των αποκλίσεων", 0, 1, 'L')


    #page 6
    pdf.add_page(orientation='L')
    pdf.set_font('DejaVuB', '', 14)
    pdf.set_xy(20, 20)
    pdf.cell(0, 10, "Παράρτημα Β'", 0, 1, 'C')

    pdf.set_font('DejaVuB', '', 12)
    pdf.cell(0, 9, "Γενικές πληροφορίες διαδικασίας ανάλυσης", 0, 1, 'C')
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 9, "• Η συχνότητα αναφοράς δεδομένων (report interval) είναι 1 δείγμα ανά 1 λεπτό ", 0, 1, 'L')
    pdf.multi_cell(0, 9, "• Για την αναγωγή των δεδομένων σε συχνότητα 10λεπτων υπολογίζεται ανά φάση η μέση τιμή των δειγμάτων", 0, 1, 'L')
    pdf.multi_cell(0, 9, "• Η αναγωγή των τιμών επί του συνόλου των φάσεων (τόσο για την τάση όσο και για την παραμόρφωση THD) προκύπτει από το μέσο όρο των 3 τιμών ανά δεκάλεπτο: ", 0, 1, 'L')
    pdf.cell(0, 9, "(total Voltage = (Voltage L1 + Voltage L2 + Voltage L3) / 3", 0, 1, 'C')
    
    pdf.set_font('DejaVu', '', 11)
    pdf.multi_cell(0, 9, "• Η συχνότητα υπολογίζεται εντός του μετρητή ως average ανά 1 λεπτό, και όχι ανά 10 δευτερόλεπτα όπως αναγράφεται στο πρότυπο 50160.", 0, 1, 'L')
    pdf.multi_cell(0, 9, "• Η ποιοτική ανάλυση τάσης στο σύνολό της ακολουθεί κατά βάση τους κανόνες/όρια/πίνακες του προτύπου 50160.", 0, 1, 'L')

    os.chdir('E:/VSprojects/thingsboard/mi6/deddhePDF/pdf_files')
    pdf.output(filename + ".pdf", 'F')
    # except:
    #    print('Unable to create pdf!')
    return




def get_dev_info(device, address):
    
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    
    # get devid by serial name
    r1 = requests.get(
        url=address + "/api/tenant/devices?deviceName=" + device,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    
    devid = r1['id']['id']
    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']

    return devid,devtoken,acc_token

def dfplots(df,var,color,ylabel):
    local_tz = pytz.timezone('Europe/Athens')
    fig, ax = plt.subplots(figsize=(20, 4.0))
    # plt.setp(ax.xaxis.get_majorticklabels())
    plt.step(df.index, df[var],color=color, where='post')
    plt.xlabel('Ημ/νία')
    plt.ylabel(ylabel)

    ax.xaxis.set( 
    major_locator = mdates.AutoDateLocator(minticks = 1, 
                                           maxticks = 5,tz=local_tz), 
    ) 
    
    locator = mdates.AutoDateLocator(minticks = 5, 
                                    maxticks = 20,tz=local_tz) 
    formatter = mdates.ConciseDateFormatter(locator,tz=local_tz) 
    ax.grid(linestyle = "dashed")
    ax.xaxis.set_major_locator(locator) 
    ax.xaxis.set_major_formatter(formatter)
    fig.tight_layout()
    plt.savefig(str(var)+'.png',dpi=150)


def alarmspdf(df,phase,al):
    fig, ax = plt.subplots()
    ax.axis('off')
    # ax1 = plt.subplot(111, aspect='equal')
    if phase=='L1':
        color='tab:blue'
    elif phase=='L2':
        color='tab:grey'
    else:
        color='tab:purple'
    
    if al=='D':
        t= ax.table(cellText=df.values, colLabels=df.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=[color,color,color,color,color,color],colWidths=[0.5 for x in df.columns])
        ax.set_title('Ανάλυση βυθίσεων '+phase,fontsize=12)
    elif al=='S':
        t= ax.table(cellText=df.values, colLabels=df.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=[color,color,color,color],colWidths=[0.5 for x in df.columns])
        ax.set_title('Ανάλυση υπερτάσεων '+phase,fontsize=12)
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df.columns))))
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    plt.gca().spines[['right', 'top']].set_visible(False)
    
    t.set_fontsize(12)
    t.scale(1, 1.5)
    fig.tight_layout()
    fig.subplots_adjust(top=0.6)
    # plt.show()
    if al=='D':
        fig.savefig('dips'+str(phase)+'.png',dpi=150,bbox_inches='tight')
    elif al=='S':
        fig.savefig('swells'+str(phase)+'.png',dpi=150,bbox_inches='tight')

def metricsfig(df):
    fig, ax = plt.subplots()
    ax.axis('off')
    
    t= ax.table(cellText=df.values, colLabels=df.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=['m','m','m','m'],colWidths=[0.5 for x in df.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df.columns))))
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    plt.gca().spines[['right', 'top']].set_visible(False)
    ax.set_title('Ποιοτικά χαρακτηριστικά τάσης *',fontsize=12)
    t.set_fontsize(12)
    t.scale(1, 1.5)
    fig.tight_layout()
    fig.subplots_adjust(top=0.6)
    fig.savefig('metrics.png',dpi=150,bbox_inches='tight')


def read_data(acc_token, devid, address, start_time, end_time, descriptors):

        
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    if r2:
        df = pd.DataFrame([])
        
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            
            df1.reset_index(drop=False, inplace=True)
            df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
            df1['ts'] = df1['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
            df1 = df1.sort_values(by=['ts'])
            df1.reset_index(drop=True, inplace=True)
            df1.set_index('ts', inplace=True, drop=True)            
            
            df = pd.concat([df, df1], axis=1)

        if df.empty:
            df = pd.DataFrame([])
        else:
            for col in df.columns:
                df[col] = df[col].astype('float64')
    else:
        df = pd.DataFrame([])
        # print('Empty json!')
    return df


def alarmdips(alarms,id):
    df = pd.DataFrame({'90>u>=80':[alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=80) & (alarms['res_perc']<90) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=200),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=80) & (alarms['res_perc']<90) & (alarms['alarm_duration']>200)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=80) & (alarms['res_perc']<90) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=1000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=80) & (alarms['res_perc']<90) & (alarms['alarm_duration']>1000)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=80) & (alarms['res_perc']<90) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()],\
                    '80>u>=70': [alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=70) & (alarms['res_perc']<80) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=200),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=70) & (alarms['res_perc']<80) & (alarms['alarm_duration']>200)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=70) & (alarms['res_perc']<80) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=1000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=70) & (alarms['res_perc']<80) & (alarms['alarm_duration']>1000)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=70) & (alarms['res_perc']<80) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()],           
                    '70>u>=40' :[alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=40) & (alarms['res_perc']<70) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=200),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=40) & (alarms['res_perc']<70) & (alarms['alarm_duration']>200)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=40) & (alarms['res_perc']<70) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=1000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=40) & (alarms['res_perc']<70) & (alarms['alarm_duration']>1000)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=40) & (alarms['res_perc']<70) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()],           
                    '40>u>=5' :[alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=5) & (alarms['res_perc']<40) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=200),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=5) & (alarms['res_perc']<40) & (alarms['alarm_duration']>200)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=5) & (alarms['res_perc']<40) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=1000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=5) & (alarms['res_perc']<40) & (alarms['alarm_duration']>1000)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=5) & (alarms['res_perc']<40) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()],           
                    '5>u' :[alarms.loc[(alarms['alarm_id']==id)  & (alarms['res_perc']<5) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=200),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id)  & (alarms['res_perc']<5) & (alarms['alarm_duration']>200)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id)  & (alarms['res_perc']<5) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=1000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']<5) & (alarms['alarm_duration']>1000)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id)  & (alarms['res_perc']<5) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()]}).transpose().reset_index()
    df.rename(columns={'index':'Ελάχιστη τάση u%',0:'10<=t<=200 ms',1:'200<t<=500 ms',2:'500<t<=1000 ms',3:'1000<t<=5000 ms',4:'5000<t<=60000 ms'}, inplace=True)
    return df


def alarmswells(alarms,id):
    df = pd.DataFrame({'u>=120':[alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=120) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=120)  & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=120)  & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()],
                    '120>u>=110': [alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=110) & (alarms['res_perc']<120) & (alarms['alarm_duration']>=10)& (alarms['alarm_duration']<=500),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=110) & (alarms['res_perc']<120) & (alarms['alarm_duration']>500)& (alarms['alarm_duration']<=5000),'Γραμμή φάσης'].count(),\
                                alarms.loc[(alarms['alarm_id']==id) & (alarms['res_perc']>=110) & (alarms['res_perc']<120) & (alarms['alarm_duration']>5000)& (alarms['alarm_duration']<=60000),'Γραμμή φάσης'].count()]}).transpose().reset_index()
    df.rename(columns={'index':'Μέγιστη τάση u%',0:'10<=t<=200 ms',1:'200<t<=500 ms',2:'500<t<=1000 ms',3:'1000<t<=5000 ms',4:'5000<t<=60000 ms'}, inplace=True)
    
    return df
    
def main(argv):
    
    
    #input = ast.literal_eval(argv[1])
    #device = '102.408.000008'
    device = argv[1]
    

    
    interval = 1 # interval in minutes
    descriptors = 'vltA,vltB,vltC,vthdA,vthdB,vthdC,frqA,frqB,frqC'
    address = 'https://mi6.meazon.com'
    # address = 'http://localhost:8080'

    
    # set dates
    local_tz = pytz.timezone('Europe/Athens')
    # start_day = datetime.datetime(2023,11,10)
    # start_day = local_tz.localize(start_day)
    # end_day = datetime.datetime(2023,11,21)
    # end_day = local_tz.localize(end_day)
    
    # # convert to unix timestamps
    # start_time = str(int(start_day.timestamp()*1e3))
    # end_time = str(int(end_day.timestamp()*1e3))
    start_time = argv[2]
    end_time = argv[3]
    
    #start_time = input['start_time']
    #end_time = input['end_time']
    
    [devid, _, acc_token] = get_dev_info(device, address)

    timethres = 12*3600000
    svec = np.arange(int(start_time),int(end_time),timethres)
    df = pd.DataFrame([])

    for st in svec:
        en = st+timethres-1
        
        if int(end_time)-en<=0: en = int(end_time)
        tmp = read_data(acc_token, devid, address,  str(st), str(en), descriptors)
        if not tmp.empty:
            tmp = tmp.resample(str(interval)+'T').mean()
            tmp = tmp.dropna()
            df = pd.concat([df,tmp])
    
    
    del tmp
    df.sort_index(inplace=True)

    df['totalVlt'] = (df['vltA']+df['vltB']+df['vltC'])/3
    df['totalVthd'] = (df['vthdA']+df['vthdB']+df['vthdC'])/3
    df['deviation'] = ((df['totalVlt']-230)/230)*100

    df['difA'] = np.abs(df['vltA']-df['totalVlt'])
    df['difB'] = np.abs(df['vltB']-df['totalVlt'])
    df['difC'] = np.abs(df['vltC']-df['totalVlt'])
    df['imbalance'] = (df[['difA', 'difB', 'difC']].max(axis=1)/df['totalVlt'])*100

    if 'frqA' in df.columns:
        frqflag=1
        df['efrq'] = (df['frqA']+df['frqB']+df['frqC'])/3
        df['frqdev'] = ((df['efrq']-50)/50)*100
    else:
        frqflag=0

    # os.chdir('/home/azureuser/deddhePDF/plots/')

    # Calculate power quality metrics based on EN50160
    passmetrics = {}
    df = df.resample('10T').mean()

    df.drop(['difA','difB','difC'], axis=1, inplace=True)
    dev95 = np.round(np.abs(df['deviation']).quantile(0.95),3)
    dev100pos = np.round(df['deviation'].max(),3)
    dev100neg = np.round(df['deviation'].min(),3) if df['deviation'].min()<0 else 0
    vimb95 = np.round(df['imbalance'].quantile(.95),3)
    vthd95 = np.round(df['totalVthd'].quantile(.95),3)
    
    passmetrics['dev95']= 'Pass' if dev95<10 else 'Fail'
    passmetrics['dev100pos']= 'Pass' if dev100pos<10 else 'Fail'
    passmetrics['dev100neg']= 'Pass' if dev100neg>-15 else 'Fail'
    passmetrics['vimb95']= 'Pass' if vimb95<2 else 'Fail'
    passmetrics['vthd95']= 'Pass' if vthd95<8 else 'Fail'

    if frqflag==1:
        frqdev995 = np.round(df['frqdev'].quantile(0.995),3)
        frqdev100pos = np.round(df['frqdev'].max(),3)
        frqdev100neg = np.round(df['frqdev'].min(),3) if df['frqdev'].min()<0 else 0

        passmetrics['frqdev995']= 'Pass' if frqdev995<1 else 'Fail'
        passmetrics['frqdev100pos']= 'Pass' if frqdev100pos<4 else 'Fail'
        passmetrics['frqdev100neg']= 'Pass' if frqdev100neg>-6 else 'Fail'

        dfplots(df,'efrq','m','Hz')
        dfplots(df,'frqdev','tab:olive','Απόκλιση %')
    
    # line plots
    dfplots(df,'totalVlt','b','Volt')
    dfplots(df,'deviation','tab:orange','Απόκλιση %')
    dfplots(df,'imbalance','tab:green','Voltage imbalance %')
    dfplots(df,'totalVthd','tab:purple','THD %')

    # extract date info
    month_Name = df.index[0].month
    month_Name2 = df.index[-1].month
    day1 = df.index[0].day
    day2 = df.index[-1].day
    year = df.index[0].year
    year2 = df.index[-1].year

    
    df = df.reset_index()
    if frqflag==1:
        df.rename(columns={'ts':'Ημ/νία','totalVlt':'Συνολική τάση','totalVthd':'Συνολική αρμονική παραμόρφωση τάσης %','deviation':'Απόκλιση τάσης %','imbalance':'Ασυμμετρία τάσης %','frqdev':'Απόκλιση συχνότητας %'}, inplace=True)
        # metrics table
        metrics = pd.DataFrame({'Μέγιστη απόκλιση τάσης στο 95% των 10λεπτων':[str(dev95),'±10%',passmetrics['dev95']], 'Μέγιστη θετική απόκλιση τάσης στο 100% των 10λεπτων':[str(dev100pos),'+10%',passmetrics['dev100pos']], 'Μέγιστη αρνητική απόκλιση τάσης στο 100% των 10λεπτων':[str(dev100neg),'-15%',passmetrics['dev100neg']],\
                                 'Μέγιστη ασυμμετρία τάσης στο 95% των 10λεπτων':[str(vimb95),'2%',passmetrics['vimb95']],\
                        'Μέγιστη αρμονική παραμόρφωση στο 95% των 10λεπτων':[str(vthd95),'8%',passmetrics['vthd95']],'Μέγιστη απόκλιση συχνότητας στο 99.5% των 10λεπτων':[str(frqdev995),'±1%',passmetrics['frqdev995']],'Μέγιστη θετική απόκλιση συχνότητας στο 100% των 10λεπτων':[str(frqdev100pos),'+4%',passmetrics['frqdev100pos']],\
                            'Μέγιστη αρνητική απόκλιση συχνότητας στο 100% των 10λεπτων':[str(frqdev100neg),'-6%',passmetrics['frqdev100neg']]}).transpose().reset_index()
        
    else:
        df.rename(columns={'ts':'Ημ/νία','totalVlt':'Συνολική τάση','totalVthd':'Συνολική αρμονική παραμόρφωση τάσης %','deviation':'Απόκλιση τάσης %','imbalance':'Ασυμμετρία τάσης %'}, inplace=True)
        # metrics table
        metrics = pd.DataFrame({'Μέγιστη απόκλιση τάσης στο 95% των 10λεπτων':[str(dev95),'±10%',passmetrics['dev95']], 'Μέγιστη θετική απόκλιση τάσης στο 100% των 10λεπτων':[str(dev100pos),'+10%',passmetrics['dev100pos']], 'Μέγιστη αρνητική απόκλιση τάσης στο 100% των 10λεπτων':[str(dev100neg),'-15%',passmetrics['dev100neg']],\
                                 'Μέγιστη ασυμμετρία τάσης στο 95% των 10λεπτων':[str(vimb95),'2%',passmetrics['vimb95']],\
                        'Μέγιστη αρμονική παραμόρφωση στο 95% των 10λεπτων':[str(vthd95),'8%',passmetrics['vthd95']]}).transpose().reset_index()
    
    metrics.rename(columns={'index':'Χαρακτηριστικό',0:'Τιμή %',1:'Επιτρεπόμενα όρια',2:'Pass/Fail'},inplace=True)
    metricsfig(metrics)

    

    # Alarms
    alarmDesc = 'alarm_time,alarm_id,alarm_duration,alarm_value'
    alarms = read_data(acc_token, devid, address,  start_time, end_time, alarmDesc)
    if not alarms.empty:
        alarms = alarms.sort_index()
        alarms['alarm_id'] = alarms['alarm_id'].astype(int)
        alarms['alarm_time'] = pd.to_datetime(alarms['alarm_time'], unit='ms')
        alarms['alarm_time'] = alarms['alarm_time'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
        alarms = alarms[(alarms['alarm_id']>1) & (alarms['alarm_id']<8)]
        alarms['Γραμμή φάσης'] = ''
        #alarms['Τύπος συμβάντος'] = ''
        #alarms['Χαρακτηρισμός διάρκειας'] = ''
        alarms['res_perc'] = 100*alarms['alarm_value']/230  # residual percentage

        # alarms.loc[ ((alarms['alarm_id']<5) & (alarms['alarm_value']>=0.1*230)),'Τύπος συμβάντος'] = 'Βύθιση τάσης'
        # alarms.loc[ ((alarms['alarm_id']<5) & (alarms['alarm_value']<0.1*230)),'Τύπος συμβάντος'] = 'Διακοπή'
        # alarms.loc[(alarms['alarm_id']>=5),'Τύπος συμβάντος'] = 'Υπέρταση'

        alarms.loc[((alarms['alarm_id']==2) | (alarms['alarm_id']==5)),'Γραμμή φάσης'] = 'L1'
        alarms.loc[((alarms['alarm_id']==3) | (alarms['alarm_id']==6)),'Γραμμή φάσης'] = 'L2'
        alarms.loc[((alarms['alarm_id']==4) | (alarms['alarm_id']==7)),'Γραμμή φάσης'] = 'L3'
        dipsA = alarmdips(alarms,2)
        dipsB = alarmdips(alarms,3)
        dipsC = alarmdips(alarms,4)
        
        swellsA = alarmswells(alarms,5)
        swellsB = alarmswells(alarms,6)
        swellsC = alarmswells(alarms,7)

    else:
        alarms = pd.DataFrame([{'Διακοπές - Μικρής διάρκειας':0,'Διακοπές - Μεγάλης διάρκειας':0,'Βυθίσεις - Στιγμιαίες':0,'Βυθίσεις - Μικρής διάρκειας':0,'Βυθίσεις - Μεγάλης διάρκειας':0,'Υπερτάσεις - Στιγμιαίες':0,'Υπερτάσεις - Μικρής διάρκειας':0,'Υπερτάσεις - Μεγάλης διάρκειας':0}]).transpose().reset_index()
        alarmsA = alarms.rename(columns={'index':'Τύπος σφαλμάτων L1',0:'Πλήθος'})
        alarmsB = alarms.rename(columns={'index':'Τύπος σφαλμάτων L2',0:'Πλήθος'})
        alarmsC = alarms.rename(columns={'index':'Τύπος σφαλμάτων L3',0:'Πλήθος'})
     
    alarmspdf(dipsA,'L1','D')
    alarmspdf(dipsB,'L2','D')
    alarmspdf(dipsC,'L3','D')
    
    alarmspdf(swellsA,'L1','S')
    alarmspdf(swellsB,'L2','S')
    alarmspdf(swellsC,'L3','S')
    filename = str(day1)+'_'+str(month_Name)+'_'+str(year)+'_'+str(day2)+'_'+str(month_Name2)+'_'+str(year2)+'_'+str(device)
    create_pdf(filename, month_Name, month_Name2, year, year2, day1,day2,device,frqflag)
    


    
    # delete plots
    # files = glob.glob('/home/azureuser/deddhePDF/plots/*')
    # for f in files:
    #     os.remove(f)

    
if __name__ == '__main__':
    sys.exit(main(sys.argv))
