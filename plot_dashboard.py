# Project Programming 2 -- Set up a Data Server

#################
# Mattia Biancaterra
#       mattia.bincaterra@usi.ch
# Pietro Bonazzi
#       pietro.bonazzi@usi.ch
# Luca Colzani
#       luca.colzani@usi.ch
# Giovanni Gizzi
#       giovanni.gizzi@usi.ch
#################

#################
# 
# Reference official plotly site sample codes:
#    -   https://plotly.com/python/line-charts/
#    -   https://dash.plotly.com/datatable
                               


from dash import Dash, dcc, html, Input, Output
from sqlalchemy import create_engine
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd



# Connection to DBproject database
cnx = create_engine( "mysql+mysqldb://{userid}:{password}@localhost/{my_database}".format(
                userid='p2', 
                password='*****',
                my_database='DBproject') )
print(cnx)

# Import data 
query = "SELECT * FROM vol_idx"
df_vol = pd.read_sql_query(query, cnx)

# Layer 1 tokens
tokens = ['ADA', 'ALGO', 'ATOM', 'AVAX', 'DOT', 'ETH',
                'FTM', 'HBAR', 'LUNA', 'NEAR', 'SOL', 'TRX' ]

# All the tables in the database have sincornyzed dates, so it's irrelevante the table from which I get the dates.
# we chose the first table in alphabetic order: ADA.
query_date = "SELECT date FROM ADA"

df_mkt = pd.DataFrame()
query = "SELECT date, mkt_cap FROM {}"
df_mkt['date'] = pd.read_sql_query(query_date, cnx)

for k in tokens:
    temp = pd.read_sql_query(query.format(k), cnx)
    temp.rename(columns={'mkt_cap':'{}'.format(k)}, inplace=True)
    df_mkt = df_mkt.merge(temp, on = 'date', how ='left')

    

df_mkt['total'] = df_mkt.drop('date', axis=1).sum(axis=1)

df_weigths = pd.DataFrame()
df_weigths['date'] = df_mkt['date']


for k in tokens:
    df_weigths[k] = df_mkt[k]/df_mkt['total']


df_correl = df_mkt.drop(['total'], axis = 1).corr().round(3)

df_vol.set_index('date', inplace = True)
df_weigths.set_index('date', inplace = True)
df_mkt.set_index('date', inplace = True)

# Initialize the Dash app
app = Dash(__name__)

# Graphs that are static, not influenced by the checklists in the webpage
# No need to update them later with a callback

# Volatility Plot
fig = px.line(df_vol)


# Heatmap Graphs
heatmap = px.imshow(df_correl, 
                    x = df_correl.index, 
                    y = df_correl.index, 
                    color_continuous_scale = 'gnbu', aspect = "auto")
# Aesthetic                    
heatmap.update_traces(text = df_correl, texttemplate = "%{text}")
heatmap.update_xaxes(side = "top", tickfont_size=18)
heatmap.update_yaxes(tickfont_size=18)
heatmap.update_layout(font={'size':14})



# Dynamic graphs with callbacks on checklist

# Market Cap L-1 only
@app.callback(
    Output("graph_mktcap", "figure"), 
    Input("checklist", "value"))
def update_line_chart(tokens):

    df_mkt['total_mktcap'] = df_mkt[tokens].sum(axis=1)
        
    fig = px.line(df_mkt['total_mktcap'])
    fig.update_yaxes(tickprefix = "$")
    return fig

# Weigths Graph
@app.callback(
    Output("graph_weigths", "figure"), 
    Input("checklist", "value"))
def update_line_chart(tokens):
    fig = px.line(df_weigths[tokens])
    return fig




# Dashboard Layout

app.layout = html.Div([
    html.H1('Layer-1 tokens dashboard', style = {'text-align':'center'}),
    html.H2(''),
    html.H2('Layer-1 Total Market Capitalization', style = {'text-align':'center'}),
    html.H5('Select the tickers you want to insert in the computation of the total market cap.'),
    
    dcc.Checklist(
        id="checklist",
        options=tokens,
        value=[t for t in tokens if t != 'ETH'],
        inline=True
    ),
    
    
    

    dcc.Graph(id="graph_mktcap"),

    dcc.Graph(id="graph_weigths"),

    html.H2("Layer-1 daily volatility index", style = {'text-align':'center'}),
    html.H4('This graph shows a marketcap-weighted daily volatility of #11 layer one tokens.'),
    dcc.Graph(figure = fig),


    html.H2("Correlation Heatmap", style = {'text-align':'center'}),
    dcc.Graph(figure = heatmap),
])





# The default port is 8050
if __name__ == "__main__":
    app.run_server(debug = False, host = '164.92.233.67')
