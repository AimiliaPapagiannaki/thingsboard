import data_fetch
import data_preprocess 
import plot_generation 
import pdf_creation
import config

#STEPS:
# 1. Fetch daily energy data for all building units (assets), Loads Klimatismos/Fotismos
# 2. Fetch all cnrg and pwr data for 102.402.002072
# 3. Fetch EnPIs when available (sq.meters/occupancy)
# 4. Fetch historical energy data & enpis for Genikos diakoptis, Amfitheatro, Planitario

def main():
    # Step 1: Fetch data
    raw_data = data_fetch.fetch_data(config.DATA_URL)

    # Step 2: Preprocess data
    df1, df2 = data_preprocess.preprocess_data(raw_data)

    # Step 3: Generate plots
    plot_generation(df1, config.OUTPUT_DIR)
    
    # Step 4: Create PDF report
    pdf_creation.create_pdf_report(config.OUTPUT_DIR)
    print(f"PDF report created at {config.OUTPUT_DIR}")

if __name__ == "__main__":
    main()