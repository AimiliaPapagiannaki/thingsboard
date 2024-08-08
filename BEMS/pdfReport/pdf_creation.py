from fpdf import FPDF
import os
import glob
import config
logos_dir = config.LOGOS_DIR

class FPDF(FPDF):
    # Page footer
    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, str(self.page_no()), 0, 0, 'C')
        self.image(logos_dir+'meazon.png', x=170, y=None, w=30, h=10)
    def header(self):
        self.set_y(10)
        self.image(logos_dir+'evgenidio.png', x=None, y=10, w=15, h=15)



def create_pdf(output_dir, pdf_dir, month, year):
    
    
    pdf = FPDF()
    try:
        pdf.add_font('DejaVuB', '', r"/usr/local/lib/python3.8/dist-packages/fpdf/fonts/DejaVuSans-Bold.ttf", uni=True)
        pdf.set_font('DejaVuB', '', 14)
        pdf.add_page()
        pdf.set_xy(30, 10)
        pdf.cell(0, 10, "Αναφορά ενεργειακής επίδοσης "+str(month)+"-"+str(year), 0, 1, 'C')
        pdf.image(output_dir+'table_rooms_breakdown.png', x=40, y=None, w=130, h=130, type='', link='')
        pdf.set_xy(40, 140)
        pdf.image(output_dir+'pie_total.png', x=30, y=None, w=170, h=125, type='', link='')
    except:
        print("page 1")
    
    try:
        pdf.add_page()
        pdf.set_xy(50, 10)
        pdf.image(output_dir+'table_specific_rooms_breakdown.png', x=30, y=None, w=130, h=100, type='', link='')
        pdf.set_xy(40, 100)
        pdf.image(output_dir+'pie_specific.png', x=30, y=None, w=170, h=125, type='', link='')
    except:
        print('page 2')
    
    try:
        pdf.add_page()
        pdf.set_xy(50, 10)
        pdf.image(output_dir+'table_enpis.png', x=30, y=None, w=150, h=150, type='', link='')
    except:
        print("page 3")

    try:
        pdf.add_page()
        pdf.set_xy(10, 30)
        pdf.image(output_dir+'monthly_linetotal.png', x=None, y=None, w=170, h=60, type='', link='')
        pdf.set_xy(10, 110)
        pdf.image(output_dir+'monthly_lineplanet.png', x=None, y=None, w=170, h=60, type='', link='')
        pdf.set_xy(10, 180)
        pdf.image(output_dir+'monthly_lineamfi.png', x=None, y=None, w=170, h=60, type='', link='')
    except:
        print("page 4")
    
    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'heatmap.png', x=40, y=None, w=130, h=115, type='', link='')
        pdf.set_xy(40, 150)
        pdf.image(output_dir+'bar_daily_Γενικός διακόπτης.png', x=30, y=None, w=150, h=100, type='', link='')
    except:
        print("page 5")

    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'bar_daily_Πλανητάριο.png', x=30, y=None, w=150, h=100, type='', link='')
        pdf.set_xy(40, 150)
        pdf.image(output_dir+'bar_daily_Αμφιθέατρο.png', x=30, y=None, w=150, h=100, type='', link='')
    except:
        print("page 6")
    
    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'bar_daily_Κλιματισμος.png', x=30, y=None, w=150, h=100, type='', link='')
        pdf.set_xy(40, 150)
        pdf.image(output_dir+'bar_daily_Φωτισμος.png', x=30, y=None, w=150, h=100, type='', link='')
    except:
        print("page 7")

    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'table_10maxpwr.png', x=40, y=None, w=130, h=220, type='', link='')
    except:
        print("page 8")

    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'table_maxnrg_split.png', x=35, y=None, w=160, h=150, type='', link='')
    except:
        print("page 9")

    try:
        pdf.add_page()
        pdf.set_xy(50, 20)
        pdf.image(output_dir+'line_power.png', x=30, y=None, w=150, h=100, type='', link='')
        pdf.set_xy(40, 150)
        pdf.image(output_dir+'bar_compaired.png', x=30, y=None, w=150, h=100, type='', link='')
    except:
        print("page 10")
    
    ##############################
    filename = 'Evgenidio_'+str(month)+'_'+str(year)+'.pdf'
    pdf.output(pdf_dir+filename , 'F')

    pngfiles = glob.glob(os.path.join(output_dir, '*.png'))
    for file_path in pngfiles:
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting {file_path}: {e}")


    
    return
    