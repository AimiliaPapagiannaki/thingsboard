import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

def create_table(df, output_dir):
    df_filtered = df.copy()
    total_load = df_filtered.loc['Γενικός διακόπτης', 'Energy consumption (kWh)']
    df_filtered['Percentage (%)'] = (df['Energy consumption (kWh)'] / total_load) * 100
    df_filtered['Room'] = df_filtered.index
    df_filtered = df_filtered.sort_values(by='Percentage (%)', ascending=False)
    df_filtered = df_filtered.reset_index(drop=True)

    # reorder columns
    columns = df_filtered.columns.tolist()
    columns = [columns[-1]] + columns[:-1]
    df_filtered = df_filtered[columns]
    print(df_filtered)

    # Create a table figure
    fig = plt.figure(figsize=(7,12))
    ax1 = plt.subplot(111, aspect='equal')
    ax1.axis('off')
    t= ax1.table(cellText=df_filtered.round(decimals=2).values, colLabels=df_filtered.columns,  loc='center',cellLoc ='center', colLoc='center', colColours=['c','c','c'],colWidths=[0.5 for x in df_filtered.columns])
    t.auto_set_font_size(False) 
    t.auto_set_column_width(col=list(range(len(df_filtered.columns))))
    
    
    table_cells = t.get_children()
    for cell in table_cells: cell.set_height(0.07)
    
    ax1.set_title('Loads percentages',fontsize=16)
    t.set_fontsize(14)
    fig.savefig(output_dir+'rooms_breakdown_table.png',dpi=150,bbox_inches='tight')


def create_table_pie(df, output_dir):
    '''Table and pie plot'''
    df_filtered = df.copy()
    df_filtered = df[df.index != 'Γενικός διακόπτης']
            
    fig = plt.figure(figsize=(10,8))
    ax = plt.subplot(111)   
    
    df_filtered = df_filtered.sort_values(by='Energy consumption (kWh)')
    # Define a custom autopct function to handle small values
    def custom_autopct(pct):
        if pct > 1:
            return f'{pct:.1f}%%'
        else:
            # Return a smaller font size for percentages <= 1%
            return f'{pct:.1f}%'
    
    wedges, _, autotexts  = ax.pie(df_filtered['Energy consumption (kWh)'].values, labels=None, colors=sns.color_palette("deep", len(df_filtered)), autopct=custom_autopct, rotatelabels=True, radius=0.9,startangle=90)


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

    ax.legend(wedges, df_filtered.index, title="Loads", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    ax.set_title('Pie chart of loads',fontsize=16)
    ax.set_ylabel('')
    fig.tight_layout()
    fig.savefig(output_dir+'totalpie.png',dpi=150,bbox_inches='tight')

def create_line_plot():
    '''Line plot'''

def create_bar_plot():
    '''Bar plot'''
    



def create_plots(pwr_data, daily_loads, daily_rooms, monthly_rooms, output_dir):
    create_table(monthly_rooms, output_dir)
    # create_table_pie(monthly_rooms, output_dir)