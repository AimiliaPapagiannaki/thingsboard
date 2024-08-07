import data_fetch
import data_preprocess 
import plot_generation 
import pdf_creation
import config
import datetime
import pytz
from dateutil.relativedelta import relativedelta
import calendar

#STEPS:
# 1. Fetch daily energy data for all building units (assets), Loads Klimatismos/Fotismos
# 2. Fetch all cnrg and pwr data for 102.402.002072
# 3. Fetch EnPIs when available (sq.meters/occupancy)
# 4. Fetch historical energy data & enpis for Genikos diakoptis, Amfitheatro, Planitario

def main():

    # Define dates of previous month
    tsnow = datetime.datetime.now()
    newts = tsnow-relativedelta(months=1)
    month = newts.month
    year = newts.year

    startm = datetime.datetime(year = year, month=month, day=1)
    endm =  startm + relativedelta(months=1)

    endm2 = startm
    startm2 = endm2 + relativedelta(months=-1)
    
    tmzn = pytz.timezone('Europe/Athens')    
    endm = tmzn.localize(endm)
    startm = tmzn.localize(startm)

    endm2 = tmzn.localize(endm2)
    startm2 = tmzn.localize(startm2)
    
    end_time = str(int((endm ).timestamp() * 1000))
    start_time = str(int((startm ).timestamp() * 1000))

    end_time2 = str(int((endm2).timestamp() * 1000))
    start_time2 = str(int((startm2).timestamp() * 1000))
    # Step 1: Fetch data
    [raw_data, prev_data, monthly_for_enpis, attrib, df_occ] = data_fetch.retrieve_raw(config.DATA_URL, start_time, end_time, tmzn, start_time2, end_time2, month)

    # Step 2: Preprocess data
    [cnrg_data, pwr_data, daily_rooms, monthly_rooms] = data_preprocess.preprocess_data(raw_data)
    # Step 3: Generate plots
    plot_generation.create_plots(cnrg_data, pwr_data, prev_data, daily_rooms, monthly_rooms, monthly_for_enpis, attrib, df_occ, config.OUTPUT_DIR)
    
    # Step 4: Create PDF report
    # pdf_creation.create_pdf_report(config.OUTPUT_DIR)
    # print(f"PDF report created at {config.OUTPUT_DIR}")

if __name__ == "__main__":
    main()