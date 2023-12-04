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
        self.image('/home/azureuser/deddhePDF/logos/meazon.png', x=10, y=10, w=30, h=10)
        self.image('/home/azureuser/deddhePDF/logos/deddie.jpg', x=250, y=10, w=30, h=10)



def create_pdf(filename, month_Name, month_Name2, year, year2,day1,day2,device,frqflag):

    try:        
        pdf = FPDF()

        # page 1
        # orientation & greek fonts
        pdf.add_page(orientation='L')
        pdf.add_font('DejaVu', '', r"/usr/local/lib/python3.8/dist-packages/fpdf/fonts/DejaVuSansCondensed.ttf", uni=True)
        pdf.add_font('DejaVuB', '', r"/usr/local/lib/python3.8/dist-packages/fpdf/fonts/DejaVuSans-Bold.ttf", uni=True)
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        # pdf.set_font('arial', 'B', 14)
        # pdf.cell(0,8, "Moxy Patras", 0, 1, 'C')
        pdf.cell(0, 10, "Î‘Î½Î¬Î»Ï…ÏƒÎ· Ï€Î¿Î¹ÏŒÏ„Î·Ï„Î±Ï‚ Ï„Î¬ÏƒÎ·Ï‚ "+str(day1)+"/"+str(month_Name)+"/"+str(year)+" - "+str(day2)+"/"+ str(month_Name2)+"/"+str(year2), 0, 1, 'C')
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0,10,"Î£ÎµÎ¹ÏÎ¹Î±ÎºÏŒÏ‚ Î±ÏÎ¹Î¸Î¼ÏŒÏ‚ Î¼ÎµÏ„ÏÎ·Ï„Î®:"+str(device), 0, 1, 'C')

        pdf.image('metrics.png', x=100, y=None, w=120, h=70, type='', link='')

        pdf.image('alarmsL1.png',x=5, y=100, w=110, h=70, type='', link='')
        pdf.image('alarmsL2.png',x=100, y=100, w=110, h=70, type='', link='')
        pdf.image('alarmsL3.png',x=200, y=100, w=110, h=70, type='', link='')

        pdf.set_xy(20, 170)
        pdf.cell(0,10,"*Î ÎµÏÎ¹Î³ÏÎ±Ï†Î® Ï…Ï€Î¿Î»Î¿Î³Î¹ÏƒÎ¼ÏŽÎ½ ÏƒÏ„Î¿ Î Î±ÏÎ¬ÏÏ„Î·Î¼Î± Î‘' & Î’'", 0, 1, 'L')


        #page 2
        pdf.add_page(orientation='L')
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        pdf.cell(0, 10, "Î¤Î¬ÏƒÎ· - Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Î¤Î¬ÏƒÎ·Ï‚ ", 0, 1, 'C')
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0,10,"Î£Ï…Î½Î¿Î»Î¹ÎºÎ® Ï„Î¬ÏƒÎ·: Average(L1,L2,L3)", 0, 1, 'L')
        pdf.image('totalVlt.png',x=5, w=280, h=65, type='', link='')

        pdf.cell(0,10,"Î Î¿ÏƒÎ¿ÏƒÏ„ÏŒ ÎºÎ±Ï„'Î±Ï€ÏŒÎ»Ï…Ï„Î¿ Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ·Ï‚ Ï„Î¬ÏƒÎ·Ï‚ Î±Ï€ÏŒ nominal (230V)", 0, 1, 'L')
        pdf.image('deviation.png',x=5, w=280, h=65, type='', link='')

        #page 3
        if frqflag==1:
            pdf.add_page(orientation='L')
            pdf.set_font('DejaVuB', '', 14)
            pdf.set_xy(20, 20)
            pdf.cell(0, 10, "Î£Ï…Ï‡Î½ÏŒÏ„Î·Ï„Î± - Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Î£Ï…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚ ", 0, 1, 'C')
            pdf.set_font('DejaVu', '', 12)
            pdf.cell(0,10,"Î£Ï…Ï‡Î½ÏŒÏ„Î·Ï„Î± Average(L1,L2,L3)", 0, 1, 'L')
            pdf.image('efrq.png',x=5, w=280, h=65, type='', link='')

            pdf.cell(0,10,"Î Î¿ÏƒÎ¿ÏƒÏ„ÏŒ ÎºÎ±Ï„'Î±Ï€ÏŒÎ»Ï…Ï„Î¿ Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ·Ï‚ ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚ Î±Ï€ÏŒ nominal (50Hz)", 0, 1, 'L')
            pdf.image('frqdev.png',x=5, w=280, h=65, type='', link='')

        #page 4
        pdf.add_page(orientation='L')
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        pdf.cell(0, 10, "Î‘ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ - Î‘ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· ", 0, 1, 'C')
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0,10,"Î‘ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ %", 0, 1, 'L')
        pdf.image('imbalance.png',x=5, w=280, h=65, type='', link='')

        pdf.cell(0,10,"Î‘ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ %", 0, 1, 'L')
        pdf.image('totalVthd.png',x=5, w=280, h=65, type='', link='')

        #page 5
        pdf.add_page(orientation='L')
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        pdf.cell(0, 10, "Î Î±ÏÎ¬ÏÏ„Î·Î¼Î± Î‘'", 0, 1, 'C')

        pdf.set_font('DejaVuB', '', 12)
        pdf.cell(0, 7.5, "Î ÎµÏÎ¹Î³ÏÎ±Ï†Î® Ï…Ï€Î¿Î»Î¿Î³Î¹ÏƒÎ¼Î¿Ï Ï‡Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÏ„Î¹ÎºÏŽÎ½ Ï„Î¬ÏƒÎ·Ï‚", 0, 1, 'C')
        pdf.multi_cell(0, 7.5, "ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚", 0, 1, 'L')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± ÎºÎ¬Î¸Îµ Ï„Î¹Î¼Î® Ï„Î·Ï‚ Î±Î½Î¬ 10Î»ÎµÏ€Ï„Î¿ Ï„Î¬ÏƒÎ·Ï‚, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Î· ÎºÎ±Ï„'Î±Ï€ÏŒÎ»Ï…Ï„Î¿ Ï€Î¿ÏƒÎ¿ÏƒÏ„Î¹Î±Î¯Î± Î´Î¹Î±Ï†Î¿ÏÎ¬ Î±Ï€ÏŒ Ï„Î± 230V --> |(x-230)/230|", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± Ï„Î¿ 100% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï…, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Ï„Î¿ max Ï„Ï‰Î½ Ï€Î±ÏÎ±Ï€Î¬Î½Ï‰ Î±Ï€Î¿ÎºÎ»Î¯ÏƒÎµÏ‰Î½.", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± Ï„Î¿ 95% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï…, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Ï„Î¿ 95o percentile Ï„Ï‰Î½ Î±Ï€Î¿ÎºÎ»Î¯ÏƒÎµÏ‰Î½ (Î”Î¹Î¬Ï„Î±Î¾Î· Î»Î¯ÏƒÏ„Î±Ï‚ Ï„Î¹Î¼ÏŽÎ½ ÏƒÎµ Î±ÏÎ¾Î¿Ï…ÏƒÎ± ÏƒÎµÎ¹ÏÎ¬ ÎºÎ±Î¹ ÎµÏ€Î¹Î»Î¿Î³Î® Ï„Î¿Ï… ÏƒÏ„Î¿Î¹Ï‡ÎµÎ¯Î¿Ï… Ï€Î¿Ï… Î¿ index Ï„Î¿Ï… Î±Î½Ï„Î¹ÏƒÏ„Î¿Î¹Ï‡ÎµÎ¯ ÏƒÏ„Î¿ 95% Ï„Î¿Ï… Î¼Î®ÎºÎ¿Ï…Ï‚ Ï„Î·Ï‚ Î»Î¯ÏƒÏ„Î±Ï‚ ÏƒÏ„Î¿Î¹Ï‡ÎµÎ¯Ï‰Î½)", 0, 1, 'L')
        
        pdf.set_font('DejaVuB', '', 12)
        pdf.multi_cell(0, 7.5, "ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚", 0, 1, 'L')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 7.5, "Î¥Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Î¿ Î¼Î­ÏƒÎ¿Ï‚ ÏŒÏÎ¿Ï‚ Ï„Ï‰Î½ 3 Ï†Î¬ÏƒÎµÏ‰Î½ ÎºÎ±Î¹ ÏƒÏ„Î· ÏƒÏ…Î½Î­Ï‡ÎµÎ¹Î± Î· Î±Ï€ÏŒÎ»Ï…Ï„Î· Î´Î¹Î±Ï†Î¿ÏÎ¬ ÎºÎ¬Î¸Îµ Ï†Î¬ÏƒÎ·Ï‚ Î±Ï€ÏŒ Ï„Î¿ Î¼Î­ÏƒÎ¿ ÏŒÏÎ¿. Î— Î±ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï€ÏÎ¿ÎºÏÏ€Ï„ÎµÎ¹ Î±Ï€ÏŒ Ï„Î· Î´Î¹Î±Î¯ÏÎµÏƒÎ· Ï„Î·Ï‚ Î¼Î­Î³Î¹ÏƒÏ„Î·Ï‚ Î´Î¹Î±Ï†Î¿ÏÎ¬Ï‚ Î¼Îµ Ï„Î¿ Î¼Î­ÏƒÎ¿ ÏŒÏÎ¿. (Î— Î¼Î­Î³Î¹ÏƒÏ„Î· Î±ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Î³Î¹Î± Ï„Î¿ 95% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï… Ï€ÏÎ¿ÎºÏÏ€Ï„ÎµÎ¹ ÏŒÏ€Ï‰Ï‚ ÎºÎ±Î¹ Ï€ÏÎ¿Î·Î³Î¿Ï…Î¼Î­Î½Ï‰Ï‚)", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "MÎ­ÏƒÎ· Ï„Î¬ÏƒÎ· -->  mean_val = (vltA + vltB + vltC)/3", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï†Î¬ÏƒÎ·Ï‚ Î‘ Î±Ï€ÏŒ Ï„Î· Î¼Î­ÏƒÎ· Ï„Î¬ÏƒÎ· --> dif1 = |(vltA - mean_val)|", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï†Î¬ÏƒÎ·Ï‚ Î’ Î±Ï€ÏŒ Ï„Î· Î¼Î­ÏƒÎ· Ï„Î¬ÏƒÎ· -->  dif2 = |(vltB - mean_val)|", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï†Î¬ÏƒÎ·Ï‚ C Î±Ï€ÏŒ Ï„Î· Î¼Î­ÏƒÎ· Ï„Î¬ÏƒÎ· -->  dif3 = |(vltC - mean_val)|", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î‘ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ --> volt_imb = (max(dif1, dif2, dif3) / mean_val) * 100", 0, 1, 'L')

        pdf.set_font('DejaVuB', '', 12)
        pdf.multi_cell(0, 7.5, "ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ·", 0, 1, 'L')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 7.5, "Î¥Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Î· Ï€Î¿ÏƒÎ¿ÏƒÏ„Î¹Î±Î¯Î± Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· Î±Î½Î¬ 10Î»ÎµÏ€Ï„Î¿, ÎºÎ±Î¹ Î· Î¼Î­Î³Î¹ÏƒÏ„Î· Î³Î¹Î± Ï„Î¿ 95% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï… Ï€ÏÎ¿ÎºÏÏ€Ï„ÎµÎ¹ ÏŒÏ€Ï‰Ï‚ ÎºÎ±Î¹ Ï€ÏÎ¿Î·Î³Î¿Ï…Î¼Î­Î½Ï‰Ï‚.", 0, 1, 'L')

        pdf.set_font('DejaVuB', '', 12)
        pdf.multi_cell(0, 7.5, "ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚", 0, 1, 'L')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± ÎºÎ¬Î¸Îµ Ï„Î¹Î¼Î® Ï„Î·Ï‚ Î±Î½Î¬ 10Î»ÎµÏ€Ï„Î¿ ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Î· ÎºÎ±Ï„'Î±Ï€ÏŒÎ»Ï…Ï„Î¿ Ï€Î¿ÏƒÎ¿ÏƒÏ„Î¹Î±Î¯Î± Î´Î¹Î±Ï†Î¿ÏÎ¬ Î±Ï€ÏŒ Ï„Î± 50Hz--> |(x-50)/50| ", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± Ï„Î¿ 100% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï…, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Ï„Î¿ max Ï„Ï‰Î½ Ï€Î±ÏÎ±Ï€Î¬Î½Ï‰ Î±Ï€Î¿ÎºÎ»Î¯ÏƒÎµÏ‰Î½.", 0, 1, 'L')
        pdf.multi_cell(0, 7.5, "Î“Î¹Î± Ï„Î¿ 95% Ï„Î¿Ï… Ï‡ÏÏŒÎ½Î¿Ï…, Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Ï„Î¿ 95o percentile Ï„Ï‰Î½ Î±Ï€Î¿ÎºÎ»Î¯ÏƒÎµÏ‰Î½", 0, 1, 'L')


        #page 6
        pdf.add_page(orientation='L')
        pdf.set_font('DejaVuB', '', 14)
        pdf.set_xy(20, 20)
        pdf.cell(0, 10, "Î Î±ÏÎ¬ÏÏ„Î·Î¼Î± Î’'", 0, 1, 'C')

        pdf.set_font('DejaVuB', '', 12)
        pdf.cell(0, 9, "Î“ÎµÎ½Î¹ÎºÎ­Ï‚ Ï€Î»Î·ÏÎ¿Ï†Î¿ÏÎ¯ÎµÏ‚ Î´Î¹Î±Î´Î¹ÎºÎ±ÏƒÎ¯Î±Ï‚ Î±Î½Î¬Î»Ï…ÏƒÎ·Ï‚", 0, 1, 'C')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 9, "â€¢ Î— ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î± Î±Î½Î±Ï†Î¿ÏÎ¬Ï‚ Î´ÎµÎ´Î¿Î¼Î­Î½Ï‰Î½ (report interval) ÎµÎ¯Î½Î±Î¹ 1 Î´ÎµÎ¯Î³Î¼Î± Î±Î½Î¬ 1 Î»ÎµÏ€Ï„ÏŒ ", 0, 1, 'L')
        pdf.multi_cell(0, 9, "â€¢ Î“Î¹Î± Ï„Î·Î½ Î±Î½Î±Î³Ï‰Î³Î® Ï„Ï‰Î½ Î´ÎµÎ´Î¿Î¼Î­Î½Ï‰Î½ ÏƒÎµ ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î± 10Î»ÎµÏ€Ï„Ï‰Î½ Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ Î±Î½Î¬ Ï†Î¬ÏƒÎ· Î· Î¼Î­ÏƒÎ· Ï„Î¹Î¼Î® Ï„Ï‰Î½ Î´ÎµÎ¹Î³Î¼Î¬Ï„Ï‰Î½", 0, 1, 'L')
        pdf.multi_cell(0, 9, "â€¢ Î— Î±Î½Î±Î³Ï‰Î³Î® Ï„Ï‰Î½ Ï„Î¹Î¼ÏŽÎ½ ÎµÏ€Î¯ Ï„Î¿Ï… ÏƒÏ…Î½ÏŒÎ»Î¿Ï… Ï„Ï‰Î½ Ï†Î¬ÏƒÎµÏ‰Î½ (Ï„ÏŒÏƒÎ¿ Î³Î¹Î± Ï„Î·Î½ Ï„Î¬ÏƒÎ· ÏŒÏƒÎ¿ ÎºÎ±Î¹ Î³Î¹Î± Ï„Î·Î½ Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· THD) Ï€ÏÎ¿ÎºÏÏ€Ï„ÎµÎ¹ Î±Ï€ÏŒ Ï„Î¿ Î¼Î­ÏƒÎ¿ ÏŒÏÎ¿ Ï„Ï‰Î½ 3 Ï„Î¹Î¼ÏŽÎ½ Î±Î½Î¬ Î´ÎµÎºÎ¬Î»ÎµÏ€Ï„Î¿: ", 0, 1, 'L')
        pdf.cell(0, 9, "(total Voltage = (Voltage L1 + Voltage L2 + Voltage L3) / 3", 0, 1, 'C')
        pdf.multi_cell(0, 9, "â€¢ ÎŸ Ï‡Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Ï„Ï‰Î½ Î²Ï…Î¸Î¯ÏƒÎµÏ‰Î½/Ï…Ï€ÎµÏÏ„Î¬ÏƒÎµÏ‰Î½ Ï‰Ï‚ ÏƒÏ„Î¹Î³Î¼Î¹Î±Î¯ÎµÏ‚/Î¼Î¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚/Î¼ÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚ Ï€ÏÎ¿ÎºÏÏ€Ï„ÎµÎ¹ Î±Ï€ÏŒ Ï„Î± Ï€Î±ÏÎ±ÎºÎ¬Ï„Ï‰ Î´Î¹Î±ÏƒÏ„Î®Î¼Î±Ï„Î±, Î²Î¬ÏƒÎµÎ¹ Ï„Î¿Ï… Ï€ÏÎ¿Ï„ÏÏ€Î¿Ï… IEEE 1159:", 0, 1, 'L')

        pdf.set_font('DejaVuB', '', 11)
        pdf.cell(0, 9, "0.5 - 30 cycles : Î£Ï„Î¹Î³Î¼Î¹Î±Î¯Î± (ÏŒÏ€Î¿Ï… cycle Î¿ ÎµÏƒÏ‰Ï„ÎµÏÎ¹ÎºÏŒÏ‚ Ï€Î»Î®ÏÎ·Ï‚ ÎºÏÎºÎ»Î¿Ï‚ 20msec)", 0, 1, 'C')    
        pdf.cell(0, 9, "0.5 - 3 sec : ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚", 0, 1, 'C')
        pdf.cell(0, 9, "3sec - 1min : ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚", 0, 1, 'C')
        pdf.set_font('DejaVu', '', 11)
        pdf.multi_cell(0, 9, "â€¢ Î— ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î± Ï…Ï€Î¿Î»Î¿Î³Î¯Î¶ÎµÏ„Î±Î¹ ÎµÎ½Ï„ÏŒÏ‚ Ï„Î¿Ï… Î¼ÎµÏ„ÏÎ·Ï„Î® Ï‰Ï‚ average Î±Î½Î¬ 1 Î»ÎµÏ€Ï„ÏŒ, ÎºÎ±Î¹ ÏŒÏ‡Î¹ Î±Î½Î¬ 10 Î´ÎµÏ…Ï„ÎµÏÏŒÎ»ÎµÏ€Ï„Î± ÏŒÏ€Ï‰Ï‚ Î±Î½Î±Î³ÏÎ¬Ï†ÎµÏ„Î±Î¹ ÏƒÏ„Î¿ Ï€ÏÏŒÏ„Ï…Ï€Î¿.", 0, 1, 'L')

        os.chdir('../pdf_files')
        pdf.output(filename + ".pdf", 'F')
    except:
       print('Unable to create pdf!')
    return


def alarmstats(alarms,line):
    df = pd.DataFrame([{'Î”Î¹Î±ÎºÎ¿Ï€Î­Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î”Î¹Î±ÎºÎ¿Ï€Î®')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î”Î¹Î±ÎºÎ¿Ï€Î­Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î”Î¹Î±ÎºÎ¿Ï€Î®')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
	'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - Î£Ï„Î¹Î³Î¼Î¹Î±Î¯ÎµÏ‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î’ÏÎ¸Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚') & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='Î£Ï„Î¹Î³Î¼Î¹Î±Î¯Î±')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î’ÏÎ¸Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î’ÏÎ¸Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - Î£Ï„Î¹Î³Î¼Î¹Î±Î¯ÎµÏ‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î¥Ï€Î­ÏÏ„Î±ÏƒÎ·Ï‚') & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='Î£Ï„Î¹Î³Î¼Î¹Î±Î¯Î±')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î¥Ï€Î­ÏÏ„Î±ÏƒÎ·Ï‚')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count(),\
         'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':\
        alarms.loc[((alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚']==line) & (alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚']=='Î¥Ï€Î­ÏÏ„Î±ÏƒÎ·Ï‚')  & (alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚']=='ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚')),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'].count()\
          }]).transpose().reset_index()
    df.rename(columns={'index':'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ†Î±Î»Î¼Î¬Ï„Ï‰Î½ '+line,0:'Î Î»Î®Î¸Î¿Ï‚'}, inplace=True)
    return df


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
    plt.xlabel('Î—Î¼/Î½Î¯Î±')
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


def alarmspdf(df,phase):
    fig, ax = plt.subplots()
    ax.axis('off')
    # ax1 = plt.subplot(111, aspect='equal')
    if phase=='L1':
        color='tab:blue'
    elif phase=='L2':
        color='tab:grey'
    else:
        color='tab:purple'
    t= ax.table(cellText=df.values, colLabels=df.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=[color,color],colWidths=[0.5 for x in df.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df.columns))))
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    plt.gca().spines[['right', 'top']].set_visible(False)
    ax.set_title('Î‘Î½Î¬Î»Ï…ÏƒÎ· ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚ '+phase,fontsize=12)
    t.set_fontsize(12)
    t.scale(1, 1.5)
    fig.tight_layout()
    fig.subplots_adjust(top=0.6)
    # plt.show()
    fig.savefig('alarms'+str(phase)+'.png',dpi=150,bbox_inches='tight')


def metricsfig(df):
    fig, ax = plt.subplots()
    ax.axis('off')
    
    t= ax.table(cellText=df.values, colLabels=df.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=['m','m'],colWidths=[0.5 for x in df.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df.columns))))
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    plt.gca().spines[['right', 'top']].set_visible(False)
    ax.set_title('Î Î¿Î¹Î¿Ï„Î¹ÎºÎ¬ Ï‡Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÏ„Î¹ÎºÎ¬ Ï„Î¬ÏƒÎ·Ï‚ *',fontsize=12)
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


def main(argv):
    
    try:
        input = ast.literal_eval(argv[1])
    except Exception as e:
        with open('errorRun.txt','w') as f:
            f.write(str(e))
    #input = ast.literal_eval(argv[1])
    #device = '102.408.000008'
    #device = argv[1]
    device = input['device']

    
    interval = 1 # interval in minutes
    descriptors = 'vltA,vltB,vltC,vthdA,vthdB,vthdC,frqA,frqB,frqC'
    # address = 'https://mi6.meazon.com'
    address = 'http://localhost:8080'

    
    # set dates
    local_tz = pytz.timezone('Europe/Athens')
    # start_day = datetime.datetime(2023,11,10)
    # start_day = local_tz.localize(start_day)
    # end_day = datetime.datetime(2023,11,21)
    # end_day = local_tz.localize(end_day)
    
    # # convert to unix timestamps
    # start_time = str(int(start_day.timestamp()*1e3))
    # end_time = str(int(end_day.timestamp()*1e3))
    #start_time = argv[2]
    #end_time = argv[3]
    
    start_time = input['start_time']
    end_time = input['end_time']
    
    [devid, _, acc_token] = get_dev_info(device, address)

    timethres = 12*3600000
    svec = np.arange(int(start_time),int(end_time),timethres)
    df = pd.DataFrame([])

    for st in svec:
        en = st+timethres-1
        
        if int(end_time)-en<=0: en = int(end_time)
        tmp = read_data(acc_token, devid, address,  str(st), str(en), descriptors)
        tmp = tmp.resample(str(interval)+'T').mean()
        tmp = tmp.dropna()
        df = pd.concat([df,tmp])
    
    
    del tmp
    df.sort_index(inplace=True)

    df['totalVlt'] = (df['vltA']+df['vltB']+df['vltC'])/3
    df['totalVthd'] = (df['vthdA']+df['vthdB']+df['vthdC'])/3
    df['deviation'] = (np.abs(df['totalVlt']-230)/230)*100

    df['difA'] = np.abs(df['vltA']-df['totalVlt'])
    df['difB'] = np.abs(df['vltB']-df['totalVlt'])
    df['difC'] = np.abs(df['vltC']-df['totalVlt'])
    df['imbalance'] = (df[['difA', 'difB', 'difC']].max(axis=1)/df['totalVlt'])*100

    if 'frqA' in df.columns:
        frqflag=1
        df['efrq'] = (df['frqA']+df['frqB']+df['frqC'])/3
        df['frqdev'] = (np.abs(df['efrq']-50)/50)*100
    else:
        frqflag=0

    os.chdir('/home/azureuser/deddhePDF/plots/')

    df = df.resample('10T').mean()

    df.drop(['difA','difB','difC'], axis=1, inplace=True)
    dev95 = np.round(df['deviation'].quantile(0.95),3)
    dev100 = np.round(df['deviation'].max(),3)
    vimb95 = np.round(df['imbalance'].quantile(.95),3)
    vthd95 = np.round(df['totalVthd'].quantile(.95),3)

    if frqflag==1:
        frqdev995 = np.round(df['frqdev'].quantile(0.995),3)
        frqdev100 = np.round(df['frqdev'].max(),3)
        dfplots(df,'efrq','m','Hz')
        dfplots(df,'frqdev','tab:olive','Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· %')
    
    # line plots
    dfplots(df,'totalVlt','b','Volt')
    dfplots(df,'deviation','tab:orange','Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· %')
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
        df.rename(columns={'ts':'Î—Î¼/Î½Î¯Î±','totalVlt':'Î£Ï…Î½Î¿Î»Î¹ÎºÎ® Ï„Î¬ÏƒÎ·','totalVthd':'Î£Ï…Î½Î¿Î»Î¹ÎºÎ® Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ %','deviation':'Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ %','imbalance':'Î‘ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ %','frqdev':'Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚ %'}, inplace=True)
        # metrics table
        metrics = pd.DataFrame([{'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(dev95), 'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 100% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(dev100), 'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(vimb95),\
                        'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(vthd95),'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚ ÏƒÏ„Î¿ 99.5% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(frqdev995),'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· ÏƒÏ…Ï‡Î½ÏŒÏ„Î·Ï„Î±Ï‚ ÏƒÏ„Î¿ 100% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(frqdev100)}]).transpose().reset_index()
    
    else:
        df.rename(columns={'ts':'Î—Î¼/Î½Î¯Î±','totalVlt':'Î£Ï…Î½Î¿Î»Î¹ÎºÎ® Ï„Î¬ÏƒÎ·','totalVthd':'Î£Ï…Î½Î¿Î»Î¹ÎºÎ® Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ %','deviation':'Î‘Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ %','imbalance':'Î‘ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ %'}, inplace=True)
        # metrics table
        metrics = pd.DataFrame([{'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(dev95), 'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±Ï€ÏŒÎºÎ»Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 100% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(dev100), 'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏƒÏ…Î¼Î¼ÎµÏ„ÏÎ¯Î± Ï„Î¬ÏƒÎ·Ï‚ ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(vimb95),\
                        'ÎœÎ­Î³Î¹ÏƒÏ„Î· Î±ÏÎ¼Î¿Î½Î¹ÎºÎ® Ï€Î±ÏÎ±Î¼ÏŒÏÏ†Ï‰ÏƒÎ· ÏƒÏ„Î¿ 95% Ï„Ï‰Î½ 10Î»ÎµÏ€Ï„Ï‰Î½':str(vthd95)}]).transpose().reset_index()
    
    metrics.rename(columns={'index':'Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÏ„Î¹ÎºÏŒ',0:'Î¤Î¹Î¼Î® %'},inplace=True)
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
        alarms['Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'] = ''
        alarms['Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚'] = ''
        alarms['Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'] = ''

        alarms.loc[ ((alarms['alarm_id']<5) & (alarms['alarm_value']>=0.1*230)),'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚'] = 'Î’ÏÎ¸Î¹ÏƒÎ· Ï„Î¬ÏƒÎ·Ï‚'
        alarms.loc[ ((alarms['alarm_id']<5) & (alarms['alarm_value']<0.1*230)),'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚'] = 'Î”Î¹Î±ÎºÎ¿Ï€Î®'
        alarms.loc[(alarms['alarm_id']>=5),'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚'] = 'Î¥Ï€Î­ÏÏ„Î±ÏƒÎ·'

        alarms.loc[((alarms['alarm_id']==2) | (alarms['alarm_id']==5)),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'] = 'L1'
        alarms.loc[((alarms['alarm_id']==3) | (alarms['alarm_id']==6)),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'] = 'L2'
        alarms.loc[((alarms['alarm_id']==4) | (alarms['alarm_id']==7)),'Î“ÏÎ±Î¼Î¼Î® Ï†Î¬ÏƒÎ·Ï‚'] = 'L3'
        alarms.loc[((alarms['alarm_duration']>5) & (alarms['alarm_duration']<500)),'Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'] = 'Î£Ï„Î¹Î³Î¼Î¹Î±Î¯Î±'
        alarms.loc[((alarms['alarm_duration']>=500) & (alarms['alarm_duration']<3000)),'Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'] = 'ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'
        alarms.loc[(alarms['alarm_duration']>=3000),'Î§Î±ÏÎ±ÎºÏ„Î·ÏÎ¹ÏƒÎ¼ÏŒÏ‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'] = 'ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚'


        alarms = alarms.drop('alarm_id',axis=1)
        alarms = alarms.reset_index(drop=True)
        alarms.rename(columns={'alarm_time':'Î—Î¼/Î½Î¯Î± ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚','alarm_duration':'Î”Î¹Î¬ÏÎºÎµÎ¹Î± (msec)','alarm_value':'Î¤Î¬ÏƒÎ· ÏƒÏ…Î¼Î²Î¬Î½Ï„Î¿Ï‚'},inplace=True)
        alarmsA = alarmstats(alarms,'L1')
        alarmsB = alarmstats(alarms,'L2')
        alarmsC = alarmstats(alarms,'L3')
    else:
        alarms = pd.DataFrame([{'Î”Î¹Î±ÎºÎ¿Ï€Î­Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0,'Î”Î¹Î±ÎºÎ¿Ï€Î­Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0,'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - Î£Ï„Î¹Î³Î¼Î¹Î±Î¯ÎµÏ‚':0,'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0,'Î’Ï…Î¸Î¯ÏƒÎµÎ¹Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0,'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - Î£Ï„Î¹Î³Î¼Î¹Î±Î¯ÎµÏ‚':0,'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - ÎœÎ¹ÎºÏÎ®Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0,'Î¥Ï€ÎµÏÏ„Î¬ÏƒÎµÎ¹Ï‚ - ÎœÎµÎ³Î¬Î»Î·Ï‚ Î´Î¹Î¬ÏÎºÎµÎ¹Î±Ï‚':0}]).transpose().reset_index()
        alarmsA = alarms.rename(columns={'index':'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ†Î±Î»Î¼Î¬Ï„Ï‰Î½ L1',0:'Î Î»Î®Î¸Î¿Ï‚'})
        alarmsB = alarms.rename(columns={'index':'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ†Î±Î»Î¼Î¬Ï„Ï‰Î½ L2',0:'Î Î»Î®Î¸Î¿Ï‚'})
        alarmsC = alarms.rename(columns={'index':'Î¤ÏÏ€Î¿Ï‚ ÏƒÏ†Î±Î»Î¼Î¬Ï„Ï‰Î½ L3',0:'Î Î»Î®Î¸Î¿Ï‚'})
     
    alarmspdf(alarmsA,'L1')
    alarmspdf(alarmsB,'L2')
    alarmspdf(alarmsC,'L3')
    
    filename = str(day1)+'_'+str(month_Name)+'_'+str(year)+'_'+str(day2)+'_'+str(month_Name2)+'_'+str(year2)+'_'+str(device)
    create_pdf(filename, month_Name, month_Name2, year, year2, day1,day2,device,frqflag)
    


    
    # delete plots
    files = glob.glob('/home/azureuser/deddhePDF/plots/*')
    for f in files:
        os.remove(f)

    
if __name__ == '__main__':
    sys.exit(main(sys.argv))
