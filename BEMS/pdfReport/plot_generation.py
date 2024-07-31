import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import calendar
import logging
import warnings
import matplotlib
# Set the logging level to WARNING to suppress informational messages
logging.getLogger('matplotlib').setLevel(logging.WARNING)
warnings.filterwarnings("ignore")
matplotlib.rcParams['interactive'] = False


def create_pwr_table(df, output_dir):
    df_filtered = df.copy()
    df_filtered['Room'] = df_filtered.index

    df_specific = df_filtered

    df_specific  = df_specific.sort_values(by='Percentage (%)', ascending=False)
    df_specific  = df_specific.reset_index(drop=True)

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



def create_table(df, output_dir, specific_loads=None):
    df_filtered = df.copy()
    total_load = df_filtered.loc['Γενικός διακόπτης', 'Energy consumption (kWh)']
    df_filtered['Percentage (%)'] = (df['Energy consumption (kWh)'] / total_load) * 100
    df_filtered['Room'] = df_filtered.index

    if specific_loads:
        df_specific = df_filtered.loc[specific_loads + ['Γενικός διακόπτης']]
        specific_loads_sum = df_specific['Energy consumption (kWh)'].sum()
        others_load = total_load - specific_loads_sum + df_specific.loc['Γενικός διακόπτης', 'Energy consumption (kWh)']
        others_row = pd.DataFrame([[ others_load, (others_load / total_load) * 100, 'Λοιπά Φορτία Εγκατάστασης']], columns=df_specific.columns, index=['Λοιπά Φορτία Εγκατάστασης'])
        df_specific = pd.concat([df_specific, others_row])
    else:
        df_specific = df_filtered


    df_specific  = df_specific.sort_values(by='Percentage (%)', ascending=False)
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

    ax.legend(wedges, df_specific.index, title="Loads", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

    pie_title = 'PieChart Πλανηταρίου/Αμφιθεάτρου/Λοιπών Φορτίων' if specific_loads else 'PieChart χώρων Ευγενιδείου'
    ax.set_title(pie_title, fontsize=14)
    ax.set_ylabel('')
    fig.tight_layout()

    file_name = 'pie_specific.png' if specific_loads else 'pie_total.png'
    fig.savefig(output_dir+file_name,dpi=150,bbox_inches='tight')

def create_line_plot():
    '''Line plot'''

def create_bar_plot(df, output_dir):
    '''Bar plot'''
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

    month = monthdict[calendar.month_name[df.index[0].month]]

    colorlist = ['mediumseagreen','royalblue','olivedrab','chocolate','darkmagenta']
    i=0
    for room in ['Γενικός διακόπτης','Πλανητάριο','Αμφιθέατρο','ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ']:

        fig, ax = plt.subplots(figsize=(7.5, 5.0))
        # plt.setp(ax.xaxis.get_majorticklabels())
        plt.bar(df.index.day, df[room],color=colorlist[i])
        plt.xlabel('Days of month')
        plt.ylabel('Energy [kWh]')
        plt.xticks(df.index.day,rotation=45)
        plt.title(room.capitalize()+': Ημερήσια κατανάλωση ('+month+')',fontsize=16)
        fig.tight_layout()
        fig.savefig(output_dir+'bar_daily_'+room.capitalize()+'.png',dpi=150)

        i += 1
    return month
    



def create_plots(pwr_data, daily_rooms, monthly_rooms, output_dir):
    # Rooms breakdown tables & pie charts
    specific_loads = ['Πλανητάριο', 'Αμφιθέατρο']
    create_table(monthly_rooms, output_dir)
    create_table(monthly_rooms, output_dir, specific_loads=specific_loads)
    create_pie(monthly_rooms, output_dir)
    create_pie(monthly_rooms, output_dir, specific_loads=specific_loads)

    # Bar charts
    create_bar_plot(daily_rooms, output_dir)

    # Active power plots
    create_pwr_table(pwr_data, output_dir)