from fpdf import FPDF

class FPDF(FPDF):
    # Page footer
    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, str(self.page_no()), 0, 0, 'C')
        #self.set_y(-10)
  #      self.image('picturemessage_vinnf3lu.tl2.png', x=180, y=None, w=25, h=8)
    def header(self):
        self.set_y(10)
        self.image('/home/mcubeazon/HttpServer_Andreas/serverFiles/meazon.png', x=10, y=None, w=30, h=10)
        
    def footer(self):
        # Go to 1.5 cm from bottom
        self.set_y(-15)
        # Print centered page number
        self.cell(0, 10, 'Designed by Vassilis Barberis', 0, 0, 'C')



def create_pdf(path, filename, month_Name, year,flag):
        
    pdf = FPDF()
    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0,8, "Moxy Patras", 0, 1, 'C')
    pdf.cell(0, 10, "Summarized activity of month: "+ month_Name+" "+str(year), 0, 1, 'C')

    pdf.set_font('arial', '', 12)
    str9 = " "
    pdf.write(5, str9)

    pdf.set_xy(15, 40)
    
    if not flag:
        pdf.image('tablepie.png', x=30, y=None, w=150, h=120, type='', link='')
    else:
        pdf.image('tablepie.png', x=30, y=None, w=150, h=140, type='', link='')
    
    pdf.add_page(orientation='L')
    #
    
    #pdf.add_page()
    
    pdf.set_xy(40, 30)
    
    if not flag:
        pdf.image('moxypie.png', x=40, y=None, w=160, h=150, type='', link='')
    else:
        pdf.image('moxypie.png', x=60, y=None, w=160, h=150, type='', link='')
    # page 2
    
    pdf.add_page()
    
    pdf.set_xy(10, 30)
    pdf.set_font('arial', 'B', 12)
    pdf.image('EnPis.png', x=35, y=None,w=160, h=80, type='', link='')
    
    
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('energy_room.png', x=None, y=None, w=180, h=140, type='', link='')
    
    
    #page 3
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.image('heatmap_nrg.png', x=None, y=None, w=190, h=140, type='', link='')
    

    #page 3
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30) 
    
    pdf.image('monthly_Total.png', x=50, y=None, w=190, h=140, type='', link='')
    
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.image('monthlyPwr_Total.png', x=50, y=None, w=190, h=140, type='', link='')
    
    #page 3
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30) 
    
    pdf.image('monthly_AirCondition.png', x=50, y=None, w=190, h=140, type='', link='')
    
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.image('monthlyPwr_AirCondition.png', x=50, y=None, w=190, h=140, type='', link='')
    
    
    #page 3
    pdf.add_page()
    pdf.set_xy(20, 30)
    pdf.image('monthlyStats_total.png', x=35, y=None, w=140, h=65, type='', link='')
    
    pdf.set_xy(20, 140)
    pdf.image('10maxPwr_Total.png', x=35, y=None, w=150, h=130, type='', link='')

    #page 3
    pdf.add_page()
    pdf.set_xy(20, 30)
    pdf.image('monthlyStats_air.png', x=35, y=None, w=140, h=65, type='', link='')
    
    pdf.set_xy(20, 140)
    pdf.image('10maxPwr_AirCondition.png', x=30, y=None, w=150, h=130, type='', link='')    
    
    #page 4
    pdf.add_page()
    pdf.set_xy(20, 30)
    
    pdf.image('dailyStats_Total.png', x=40, y=None, w=120, h=240, type='', link='')
    
     #page 4
    pdf.add_page()
    pdf.set_xy(20, 30)
    pdf.image('dailyStats_AirCondition.png', x=40, y=None, w=120, h=240, type='', link='')
    
    
    
    
    
    
    #page 5
    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.image('boxplot_Total.png', x=None, y=None, w=190, h=150, type='', link='')

    pdf.add_page(orientation='L')
    pdf.set_xy(50, 30)
    pdf.image('boxplot_AirCondition.png', x=None, y=None, w=190, h=150, type='', link='')
    
    
    ##############################
    pdf.output(filename + ".pdf", 'F')
    
    return
    