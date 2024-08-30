import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import calendar
import matplotlib.dates as mdates




def create_pwr_table(df, output_dir):
    df.loc[df['totalpwr']<0, 'totalpwr'] = 0
    
    stats = {}
    stats['Μέγιστη ισχύς (kW)'] = df['totalpwr'].max()
    stats['Ελάχιστη ισχύς (kW)'] = df['totalpwr'].min()
    stats['Μέση ισχύς (kW)'] = df['totalpwr'].mean()
    stats_df = pd.DataFrame(list(stats.items()), columns=['Στατιστικό μέτρο', 'Τιμή'])


    df2 = df.resample('1H').max()
    df2 = df2.dropna()
    df2 = df2.sort_values(by='totalpwr', ascending=False)
    df2 = df2.rename(columns={'totalpwr':'Ενεργός ισχύς (kW)'})
    df2 = df2.iloc[:10]
    df2['Ημ/νία'] = df2.index
    df2  = df2.reset_index(drop=True)

    # Define figure size
    fig_height = 10
    fig = plt.figure(figsize=(12, fig_height))

    # Create the first subplot
    ax1 = plt.subplot(211, aspect='equal')
    ax1.axis('off')
    t1 = ax1.table(cellText=stats_df.round(decimals=2).values,
                colLabels=stats_df.columns,
                loc='center', cellLoc='center',
                colLoc='center',
                colColours=['skyblue', 'skyblue'],
                colWidths=[0.5 for x in stats_df.columns])
    t1.auto_set_font_size(True)
    t1.auto_set_column_width(col=list(range(len(stats_df.columns))))

    table_cells = t1.get_children()
    for cell in table_cells:
        cell.set_height(0.08)
 
    table_title = 'Στατιστικά ισχύος'
    ax1.set_title(table_title, fontsize=14, pad=2)
    t1.auto_set_column_width(col=list(range(len(stats_df.columns))))

    # Create the second subplot
    ax2 = plt.subplot(212, aspect='equal')
    ax2.axis('off')
    t2 = ax2.table(cellText=df2.round(decimals=2).values,
                colLabels=df2.columns,
                loc='center', cellLoc='center',
                colLoc='center',
                colColours=['skyblue', 'skyblue', 'skyblue'],
                colWidths=[0.5 for x in df2.columns])
    t2.auto_set_font_size(True)
    t2.auto_set_column_width(col=list(range(len(df2.columns))))

    table_cells = t2.get_children()
    for cell in table_cells:
        cell.set_height(0.08)

    table_title = 'Μέγιστες τιμές ισχύος (kW)'
    ax2.set_title(table_title, fontsize=14, pad=10)

    # Adjust spacing between subplots
    plt.subplots_adjust(hspace=0.1)

    # Save the figure
    file_name = 'table_10maxpwr.png'
    fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight', pad_inches=1)

    

def create_table(df, output_dir, specific_loads=None):
    df_filtered = df.copy()
    total_load = df_filtered.loc['Γενικός διακόπτης', 'Energy consumption (kWh)']
    df_filtered['Ποσοστό %'] = (df['Energy consumption (kWh)'] / total_load) * 100
    df_filtered['Δομική ενότητα'] = df_filtered.index

    if specific_loads:
        df_specific = df_filtered.loc[specific_loads + ['Γενικός διακόπτης']]
        specific_loads_sum = df_specific['Energy consumption (kWh)'].sum()
        others_load = total_load - specific_loads_sum + df_specific.loc['Γενικός διακόπτης', 'Energy consumption (kWh)']
        others_row = pd.DataFrame([[ others_load, (others_load / total_load) * 100, 'Λοιπά Φορτία Εγκατάστασης']], columns=df_specific.columns, index=['Λοιπά Φορτία Εγκατάστασης'])
        df_specific = pd.concat([df_specific, others_row])
    else:
        df_specific = df_filtered


    df_specific  = df_specific.sort_values(by='Ποσοστό %', ascending=False)
    df_specific  = df_specific.reset_index(drop=True)

    # reorder columns
    columns = df_specific.columns.tolist()
    columns = [columns[-1]] + columns[:-1]
    df_specific = df_specific[columns]

    # Determine figure size based on number of rows
    num_rows = len(df_specific)
    fig_height = 0.6 * num_rows + 2  # Adjust the multiplier and base value as needed


    # Create a table figure
    # fig = plt.figure(figsize=(7,12))
    fig = plt.figure(figsize=(7, fig_height))
    ax1 = plt.subplot(111, aspect='equal')
    ax1.axis('off')
    t= ax1.table(cellText=df_specific.round(decimals=2).values, 
                 colLabels=df_specific.columns,  
                 loc='center',cellLoc ='center', 
                 colLoc='center', 
                #  colColours=['tab:gray','mediumseagreen','mediumseagreen'],
                 colColours=['tab:gray','tab:gray','tab:gray'],
                 colWidths=[0.5 for x in df_specific.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df_specific.columns))))
    
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    
    table_title = 'Κατανάλωση Πλανηταρίου/Αμφιθεάτρου' if specific_loads else 'Κατανάλωση χώρων Ευγενιδείου'
    ax1.set_title(table_title,fontsize=14, pad=20)
    t.auto_set_column_width(col=list(range(len(df_specific.columns))))
    
    file_name = 'table_specific_rooms_breakdown.png' if specific_loads else 'table_rooms_breakdown.png'
    fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight', pad_inches=1)


def create_pie(df, output_dir, specific_loads=None):
    '''Table and pie plot'''
    df_filtered = df.copy()
    df_filtered = df[df.index != 'Γενικός διακόπτης']
            
    if specific_loads:
        df_specific = df_filtered.loc[specific_loads]
        other_loads_sum = df_filtered.loc[~df_filtered.index.isin(specific_loads)].sum()
        other_loads = pd.DataFrame([other_loads_sum], index=['Λοιπά Φορτία Εγκατάστασης'])
        df_specific = pd.concat([df_specific, other_loads])
    else:
        df_specific = df_filtered
    
    fig = plt.figure(figsize=(10,8))
    ax = plt.subplot(111)   
    
    df_specific = df_specific.sort_values(by='Energy consumption (kWh)', ascending=False)
    # Define a custom autopct function to handle small values
    def custom_autopct(pct):
        if pct > 1:
            return f'{pct:.1f}%%'
        else:
            # Return a smaller font size for percentages <= 1%
            return f'{pct:.1f}%'
    
    wedges, _, autotexts  = ax.pie(df_specific['Energy consumption (kWh)'].values, labels=None, colors=sns.color_palette("tab20", len(df_specific)), autopct=custom_autopct, rotatelabels=True, radius=0.9,startangle=90)


    # Set font size for all autotexts, and change small percentage texts
    for autotext, wedge in zip(autotexts, wedges):
        text = autotext.get_text()
        angle = (wedge.theta2 - wedge.theta1) / 2.0 + wedge.theta1  # Midpoint of the wedge
        rotation = angle if angle < 180 else angle - 360  # Rotate text to be within the pie slice
        autotext.set_rotation(rotation)

        if float(text.strip('%')) <= 1.0:
            autotext.set_fontsize(6)  # Set smaller font size for small percentages
        else:
            autotext.set_fontsize(8)  # Set default font size for other percentages

    ax.legend(wedges, df_specific.index, title="Δομικές ενότητες", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

    pie_title = 'PieChart Πλανηταρίου/Αμφιθεάτρου/Λοιπών Φορτίων' if specific_loads else 'PieChart χώρων Ευγενιδείου'
    ax.set_title(pie_title, fontsize=14)
    ax.set_ylabel('')
    fig.tight_layout()

    file_name = 'pie_specific.png' if specific_loads else 'pie_total.png'
    fig.savefig(output_dir+file_name,dpi=150,bbox_inches='tight')


def create_line_plot_pwr(df, output_dir, monthdict):
    month = monthdict[calendar.month_name[df.index[0].month]]

    '''Line plot for min/max/mean power'''
    mindf = df.resample('1H').min().rename(columns={'totalpwr':'Minimum power (kW)'})
    mindf = mindf.resample('1D').min()

    maxdf = df.resample('1H').max().rename(columns={'totalpwr':'Maximum power (kW)'})
    maxdf = maxdf.resample('1D').max()

    avgdf = df.resample('1H').mean().rename(columns={'totalpwr':'Average power (kW)'})
    avgdf = avgdf.resample('1D').mean()

    avg_df = pd.concat([avgdf, mindf, maxdf], axis=1)
    
    # Define the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plot each line with different colors and markers
    ax.plot(avg_df.index.day, avg_df['Minimum power (kW)'], label='Ελάχιστη', color='yellowgreen', marker='o', linestyle='-', markersize=8)
    ax.plot(avg_df.index.day, avg_df['Maximum power (kW)'], label='Μέγιστη', color='brown', marker='s', linestyle='--', markersize=8)
    ax.plot(avg_df.index.day, avg_df['Average power (kW)'], label='Μέση', color='royalblue', marker='^', linestyle='-.', markersize=8)

    # Add a legend
    ax.legend()

    # Add labels and title
    # ax.set_xlabel('Ημέρες')
    ax.set_ylabel('Ενεργός ισχύς (kW)')
    ax.set_title('Ημερήσια μέση, ελάχιστη, μέγιστη ισχύς ('+month+')')
    plt.xticks(avg_df.index.day)
    # Save the figure
    
    file_name = 'line_power.png'
    fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight')


def create_bar_plot(df, output_dir, monthdict):
    '''Bar plot'''
    

    month = monthdict[calendar.month_name[df.index[0].month]]

    colorlist = ['mediumseagreen','royalblue','darkmagenta','chocolate','olivedrab']
    i=0
    for room in ['Γενικός διακόπτης','Πλανητάριο','Αμφιθέατρο','ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ']:

        fig, ax = plt.subplots(figsize=(7.5, 5.0))
        plt.bar(df.index.day, df[room],color=colorlist[i])
        # plt.xlabel('Days of month')
        plt.ylabel('Κατανάλωση ενέργειας (kWh)')
        plt.xticks(df.index.day)
        plt.title(room.capitalize()+': Ημερήσια κατανάλωση ('+month+')',fontsize=16)
        fig.tight_layout()
        fig.savefig(output_dir+'bar_daily_'+room.capitalize()+'.png',dpi=150)

        i += 1
    return month
    
    
def create_yearly_monthplot(tmp, output_dir):
    df = tmp.copy()
    df.rename(columns={'102.402.002072':'Συνολική κατανάλωση'}, inplace=True)
    df['Συνολική κατανάλωση'] = df['Συνολική κατανάλωση']/1000

    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    plt.bar(df.index.month, df['Συνολική κατανάλωση'],color='tab:blue')
    # plt.xlabel('Days of month')
    plt.ylabel('Κατανάλωση ενέργειας (MWh)')
    plt.xticks(df.index.month)
    plt.title('Συνολική κατανάλωση ανά μήνα',fontsize=16)
    fig.tight_layout()
    fig.savefig(output_dir+'bar_yearly.png',dpi=150)
    
    
def create_heatmap(df, output_dir):
    df = df/1000
    #  Reshape the data to have days as rows and hours as columns
    df['Ημέρες'] = df.index.day
    df['Ώρες'] = df.index.hour
    pivot_df = df.pivot_table(values='totalcnrg', index='Ώρες', columns='Ημέρες')
    # Ensure hours are sorted in ascending order
    pivot_df = pivot_df.sort_index(axis=0, ascending=False)

    # Create the heatmap
    plt.figure(figsize=(8, 6.5))
    sns.heatmap(pivot_df, cmap='YlGnBu', cbar_kws={'label': 'Ενέργεια (kWh)'})

    # Add labels and title
    plt.ylabel('Ώρες',fontsize=12)
    plt.xlabel('Ημέρες',fontsize=12)
    plt.title('Heatmap κατανάλωσης ενέργειας', fontsize=14)
    
    # Set the y-axis tick labels to be non-rotated
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10,rotation=0)
    # Save the figure
    plt.savefig(output_dir+'heatmap.png', dpi=150, bbox_inches='tight')

def create_2month_barplot(prev, curr, output_dir, monthdict):
    month_prev = prev.index[0].month
    month_curr = curr.index[0].month
    prev['totalCleanNrg'] = prev['totalCleanNrg']/1000 # previous month
    prev['day'] = prev.index.day
    prev.set_index('day', inplace=True, drop=True)
    curr = curr.to_frame()
    curr['day'] = curr.index.day
    curr.set_index('day', inplace=True, drop=True)    
    col_prev = 'Ενέργεια kWh ('+monthdict[calendar.month_name[month_prev]]+')'
    col_curr = 'Ενέργεια kWh ('+monthdict[calendar.month_name[month_curr]]+')'
    prev = prev.rename(columns={'totalCleanNrg':col_prev})
    curr = curr.rename(columns={'Γενικός διακόπτης':col_curr})
    prev = pd.concat([prev,curr], axis=1)

    # Number of categories
    n = len(prev)

    # The x locations for the groups
    ind = prev.index
    # The width of the bars
    width = 0.35

    fig, ax = plt.subplots(figsize=(7.5,5))
    bars1 = ax.bar(ind - width/2, prev[col_prev], width, label=monthdict[calendar.month_name[month_prev]], color='royalblue')
    bars2 = ax.bar(ind + width/2, prev[col_curr], width, label=monthdict[calendar.month_name[month_curr]], color='orange')

    ax.set_xlabel('Ημέρες')
    ax.set_ylabel('Ενέργεια (kWh)')
    ax.set_title('Ημερήσια κατανάλωση των μηνών: '+monthdict[calendar.month_name[month_prev]]+'/'+monthdict[calendar.month_name[month_curr]], fontsize=14)
    step = 2  # Change this value based on how sparse you want the ticks to be
    ax.set_xticks(ind[::step])
    ax.legend()
    fig.savefig(output_dir+'bar_compaired.png',dpi=150)


def maxPwrBreakdown(df, output_dir):
    maxnrg = df['Γενικός διακόπτης'].max()
    maxdf = df.loc[df['Γενικός διακόπτης']==maxnrg]
    maxdf = maxdf.drop(['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ'],axis=1)
    maxdate = maxdf.index[0]# index of day with max energy consumption

    maxdf = maxdf.T
    maxdf.columns = ['Μέση ωριαία ισχύς (kW)']
    maxdf['Ποσοστό %'] = 100*maxdf/maxnrg
    maxdf['Μέση ωριαία ισχύς (kW)'] = maxdf['Μέση ωριαία ισχύς (kW)']/24
    
    maxdf  = maxdf.sort_values(by='Ποσοστό %', ascending=False)
    maxdf['Δομική ενότητα'] = maxdf.index
    maxdf  = maxdf.reset_index(drop=True)

    # reorder columns
    columns = maxdf.columns.tolist()
    columns = [columns[-1]] + columns[:-1]
    maxdf = maxdf[columns]

    # Determine figure size based on number of rows
    num_rows = len(maxdf)
    fig_height = 0.6 * num_rows + 2  # Adjust the multiplier and base value as needed


    # Create a table figure
    fig = plt.figure(figsize=(7, fig_height))
    ax1 = plt.subplot(111, aspect='equal')
    ax1.axis('off')
    t= ax1.table(cellText=maxdf.round(decimals=2).values, 
                 colLabels=maxdf.columns,  
                 loc='center',cellLoc ='center', 
                 colLoc='center', 
                #  colColours=['tab:gray','mediumseagreen','mediumseagreen'],
                 colColours=['chocolate','chocolate','chocolate'],
                 colWidths=[0.5 for x in maxdf.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(maxdf.columns))))
    
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)

    date_str = maxdate.strftime('%Y-%m-%d')
    table_title = 'Επιμερισμός φορτίων στις '+date_str+', Ημερήσια κατανάλωση:'+str(maxnrg)+' kWh' 
    ax1.set_title(table_title,fontsize=14, pad=20)
    t.auto_set_column_width(col=list(range(len(maxdf.columns))))
    
    file_name = 'table_maxnrg_split.png' 
    fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight', pad_inches=1)


def create_line_plot_attr(df, attrib, df_occ, output_dir):
    
    # divide consumption with respective attribute
    df.rename(columns={'102.402.002072':'Γενικός διακόπτης'}, inplace=True)
    df['kWh/τ.μ. Κτιρίου'] = df['Γενικός διακόπτης']/attrib['sqmt']

    df = pd.concat([df,df_occ], axis=1)
    df['kWh/ώρα χρήσης (Αμφιθέατρο)'] = df['Αμφιθέατρο']/df['Χρήση Αμφιθέατρο']
    df['kWh/ώρα χρήσης (Πλανητάριο)'] = df['Πλανητάριο']/df['Χρήση Πλανητάριο']
    colordict={'kWh/τ.μ. Κτιρίου':'yellowgreen', 'kWh/ώρα χρήσης (Αμφιθέατρο)':'brown', 'kWh/ώρα χρήσης (Πλανητάριο)':'royalblue'}
    titledict={'kWh/τ.μ. Κτιρίου':'total', 'kWh/ώρα χρήσης (Αμφιθέατρο)':'amfi', 'kWh/ώρα χρήσης (Πλανητάριο)':'planet'}

    for col in ['kWh/τ.μ. Κτιρίου', 'kWh/ώρα χρήσης (Αμφιθέατρο)', 'kWh/ώρα χρήσης (Πλανητάριο)']:
        # Define the plot
        # fig, ax = plt.subplots(figsize=(10, 6))
        fig, ax = plt.subplots(figsize=(15, 4))
        # Plot each line with different colors and markers
        ax.plot(df.index, df[col], color=colordict[col], marker='o', linestyle='-', markersize=8)
        # Set x-axis major locator and formatter
        ax.xaxis.set_major_locator(mdates.MonthLocator())  # Set the locator to months
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))  # Format as 'Month Year'
        plt.xticks(rotation=90)
        ax.set_ylabel('kWh')
        ax.set_title(col)
        file_name = 'monthly_line'+titledict[col]+'.png'
        fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight')
    return df


def create_enpis_table(loads_df, all_df, attrib, output_dir):
    loads_df = loads_df.resample('MS').sum()
    all_df = pd.concat([all_df,loads_df],axis=1)
    all_df = all_df.dropna()
    
    enpis = {}
    enpis['Συνολική κατανάλωση (kWh)/τ.μ. εγκατάστασης'] = all_df['kWh/τ.μ. Κτιρίου'].max()
    enpis['Συνολική κατανάλωση Κλιματισμού (kWh)/τ.μ. εγκατάστασης'] = all_df['ΚΛΙΜΑΤΙΣΜΟΣ'].max()/attrib['sqmt']
    enpis['Συνολική κατανάλωση Φωτισμού (kWh)/τ.μ. εγκατάστασης'] = all_df['ΦΩΤΙΣΜΟΣ'].max()/attrib['sqmt']
    enpis['Μηνιαίες ώρες λειτουργίας Πλανηταρίου'] = all_df['Χρήση Πλανητάριο'].max()
    enpis['Μηνιαίες ώρες λειτουργίας Αμφιθεάτρου'] = all_df['Χρήση Αμφιθέατρο'].max()
    enpis['Κατανάλωση ανά ώρα χρήσης Πλανηταρίου (kWh)'] = all_df['kWh/ώρα χρήσης (Πλανητάριο)'].max()
    enpis['Κατανάλωση ανά ώρα χρήσης Αμφιθεάτρου (kWh)'] = all_df['kWh/ώρα χρήσης (Αμφιθέατρο)'].max()
    enpis['Κατανάλωση Πλανηταρίου & Αμφιθεάτρου/Συνολική (%)'] = (all_df['Πλανητάριο'].max()+all_df['Αμφιθέατρο'].max())/all_df['Γενικός διακόπτης'].max()
    enpis['Μηνιαίο ενεργειακό κόστος εγκατάστασης (Ευρώ)'] = all_df['Γενικός διακόπτης'].max()*attrib['budget']
    enpis = pd.DataFrame(list(enpis.items()), columns=['EnPis', 'Τιμή'])
    enpis['Τιμή'] = enpis['Τιμή'].round(decimals=2)
    
    # Determine figure size based on number of rows
    num_rows = len(enpis)
    fig_height = 0.6 * num_rows + 2  # Adjust the multiplier and base value as needed
    fig = plt.figure(figsize=(7, fig_height))
    ax1 = plt.subplot(111, aspect='equal')
    ax1.axis('off')
    t= ax1.table(cellText=enpis.values, 
                 colLabels=enpis.columns,  
                 loc='center',cellLoc ='center', 
                 colLoc='center', 
                #  colColours=['tab:gray','mediumseagreen','mediumseagreen'],
                 colColours=['royalblue','royalblue'],
                 colWidths=[0.5 for x in enpis.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(enpis.columns))))
    
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    
    table_title = 'Δείκτες ενεργειακής επίδοσης'
    ax1.set_title(table_title,fontsize=14, pad=20)
    t.auto_set_column_width(col=list(range(len(enpis.columns))))
    
    file_name = 'table_enpis.png'
    fig.savefig(output_dir+file_name, dpi=150, bbox_inches='tight', pad_inches=1)



def create_plots(cnrg_data, pwr_data, prev_data, daily_rooms, monthly_rooms, monthly_for_enpis, attrib, df_occ, output_dir):

    monthdict = {'January':'Ιανουάριος',
                 'February':'Φεβρουάριος',
                 'March':'Μάρτιος',
                 'April':'Απρίλιος',
                 'May':'Μάιος',
                 'June':'Ιούνιος',
                 'July':'Ιούλιος',
                 'August':'Αύγουστος',
                 'September':'Σεπτέμβριος',
                 'October':'Οκτώβριος',
                 'November':'Νοέμβριος',
                 'December':'Δεκέμβριος'}
    
    # Rooms breakdown tables & pie charts
    specific_loads = ['Πλανητάριο', 'Αμφιθέατρο']
    
    try:
        create_table(monthly_rooms, output_dir)
        create_table(monthly_rooms, output_dir, specific_loads=specific_loads)
    except:
        print("Unable to create tables with loads")

    try:
        create_pie(monthly_rooms, output_dir)
        create_pie(monthly_rooms, output_dir, specific_loads=specific_loads)
    except:
        print("Unable to create pies")

    # Bar charts
    try:
        create_bar_plot(daily_rooms, output_dir,monthdict)
    except:
        print("Unable to create barplots")

    # Active power plots
    try:
        create_pwr_table(pwr_data, output_dir)
        create_line_plot_pwr(pwr_data, output_dir, monthdict)
    except:
        print("Unable to create power table plot")

    # heatmap
    try:
        create_heatmap(cnrg_data, output_dir)
    except:
        print("Unable to create heatmap")

    # comparative barplot 2 months
    try:
        create_2month_barplot(prev_data, daily_rooms['Γενικός διακόπτης'], output_dir, monthdict)
    except:
        print("Unable to create comparative barplot")

    # Split loads on day with max power
    try:
        maxPwrBreakdown(daily_rooms, output_dir)
    except:
        print("Unable to create power  breakdown")
     # Yearly plot to compare monthly energy
    try:
        create_yearly_monthplot(monthly_for_enpis, output_dir)
    except:
        print("Unable to create yearly barplot")
        
    # Monthly Line charts with sqmt/occ
    try:
        monthly_for_enpis = create_line_plot_attr(monthly_for_enpis, attrib, df_occ, output_dir)
    except:
        print("Unable to create lineplot/enpis")

    # Table with EnPis
    try:
        create_enpis_table(daily_rooms[['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ']], monthly_for_enpis, attrib, output_dir)
    except:
        print("Unable to create enpis table")
