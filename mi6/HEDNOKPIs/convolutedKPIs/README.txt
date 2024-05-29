This folder contains an analysis conducted on HEDNO transformer's data for Jan, Feb, Apr, Mar and May (up to 26th) 2024.
An attempt was made to create a composite KPI consisting of #of overpower alarms*avg duration*overpower kVA.

Jupyter notebooks:
plotOverPwr.ipynb --> Basic nb to plot just the number of power alarms, as grouped bars graph.
plotKPIs.ipynb --> Composite KPI calculation and plot as an aggregated metric for all 3 phases
plotKPIs_perphase.ipynb --> The same as before, but split per phase resulting into 3 figures, and overall KPI 
	Figures: PowerKPI_phaseL1/L2/L3.png, AggPowerKpi.png
	Excel files: phaseL1/L2/L3.xlsx
plotKPIs_boxplots.ipynb --> Boxplots per transformer and month, regarding the kVA of the overpower resulting to 3 figures (boxplot_phaseL1/L2/L3)


All notebooks are based on the data extracted from TB using python scripts loop_convolKPIs.py and convolKPI.py,
stored in xlsx files HEDNO_overpwr_(month)_2024.xlsx.