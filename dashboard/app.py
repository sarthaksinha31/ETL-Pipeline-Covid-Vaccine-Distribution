import warnings
warnings.filterwarnings('ignore')
from dash import Dash, html, dcc, Input, Output
from data_visuals import *

app = Dash(__name__)
app.title = "Analysing Vaccine Stock Market Fluctuations"
company_list = ['Pfizer','Moderna','Johnson & Johnson','Novavax']

company_dropdown = html.Div(children=[
                html.Div(
                    children=[
                        # html.Div(children="Select Vaccine Company", className="menu-title"),
                        dcc.Dropdown(
                            id="company-dropdown",
                            options=[{"label": region, "value": region} for region in company_list],
                            value="Pfizer",
                            clearable=False,
                            className="dropdown",
                        ),
                    ]
                )
            ])

stock_distribution_graph = dcc.Graph(
        id='stock-distribution-graph',
        config={"displayModeBar": False},
        className = "card"
    )

stock_twitter_graph = dcc.Graph(
        id='stock-twitter-graph',
        config={"displayModeBar": False},
        className = "card"
    )

df_stock_vaccine = get_stock_distribution_data()
fig_roi = plot_roi(df_stock_vaccine)
roi_graph = dcc.Graph(
        id='roi-graph',
        config={"displayModeBar": False},
        figure=fig_roi,
        className = "card"
    )

fig_dist_change = plot_distribution_change(df_stock_vaccine)
dist_change_graph = dcc.Graph(
        id='distribution-change-graph',
        config={"displayModeBar": False},
        figure=fig_dist_change,
        className = "card"
    )

fig_vol_change = plot_volume_change(df_stock_vaccine)
vol_change_graph = dcc.Graph(
        id='volume-change-graph',
        config={"displayModeBar": False},
        figure=fig_vol_change,
        className = "card"
    )

fig_stock_volatility = plot_stock_volatility(df_stock_vaccine)
volatility_graph = dcc.Graph(
        id='volatility-graph',
        config={"displayModeBar": False},
        figure=fig_stock_volatility,
        className = "card"
    )
app.layout = html.Div(children=[
    html.H1(children='Analysing Vaccine Stock Market Fluctuations',className="header-title"),
    company_dropdown,
    stock_distribution_graph,
    stock_twitter_graph,
    roi_graph,
    dist_change_graph,
    vol_change_graph,
    volatility_graph
])

@app.callback(
    [Output(component_id='stock-distribution-graph',component_property='figure'),Output(component_id='stock-twitter-graph',component_property='figure')],
    Input(component_id='company-dropdown',component_property='value')
)
def update_graph(selected_company):
    df_month,col_name = get_stock_vs_distribution_data(selected_company)
    fig_distribution = plot_stock_vs_distribution(selected_company,df_month,col_name)
    df_tweet= get_stock_vs_tweeter_data(selected_company)
    fig_tweet = plot_stock_vs_tweeter(df_tweet,selected_company)
    return fig_distribution,fig_tweet

if __name__ == '__main__':
    app.run_server(debug=True)
