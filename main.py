import copy
from datetime import datetime  # date, timedelta,

import pandas as pd

# from db_connector import DBConnector
# import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from automate_report import AutomateReport
from pvlib_simulation import *

id_p = 1
t_start = "2022-11-04"
t_end = "2022-11-08"

automate_report_obj = AutomateReport(id_p, t_start, t_end)

df = automate_report_obj.get_active_power_from_db()
df = df.sort_values(by="t")
# df.to_excel('inverter_data.xlsx')
df_meteo_satelitte = automate_report_obj.get_meteo_satellite_from_db()


lst_id_i = df["id"].unique()
start_date = datetime.strptime(t_start, "%Y-%m-%d")
end_date = datetime.strptime(t_end, "%Y-%m-%d")
df_simulated = pd.DataFrame()

# Simulate data
for date_iter in daterange(start_date, end_date):
    print("Simulating day: ", date_iter.strftime("%Y-%m-%d"))
    df_aux = get_simulation_per_inverter(id_p, date_iter, lst_id_i)
    df_simulated = pd.concat([df_simulated, df_aux])
dict_inverter_id = df.groupby(["id"]).size().reset_index()

# Graph data
fig = make_subplots(specs=[[{"secondary_y": True}]])

lst_id_i = df["id"].unique()
for id_i in lst_id_i:
    df_iter = copy.deepcopy(df.loc[(df["id"] == id_i)])
    df_iter.reset_index(drop=True, inplace=True)
    fig.add_trace(
        go.Scatter(
            x=df_iter["t"],
            y=df_iter["active_power"],
            mode="lines",
            name=str(df_iter["id"][0]),
        ),
        secondary_y=False,
    )

fig.add_trace(
    go.Scatter(
        x=df_meteo_satelitte["t"],
        y=df_meteo_satelitte["ghi"],
        mode="lines",
        name="ghi",
    ),
    secondary_y=True,
)

for id_i in lst_id_i:
    inverter_id = dict_inverter_id.loc[dict_inverter_id["id"] == id_i]["id"]
    inverter_id = str(inverter_id.values[0])
    df_iter = copy.deepcopy(df_simulated.loc[(df_simulated["id"] == id_i)])
    df_iter.reset_index(drop=True, inplace=True)
    fig.add_trace(
        go.Scatter(
            x=df_iter["t"],
            y=df_iter["ac"],
            mode="lines",
            name="simulated_" + inverter_id,
            legendgroup="simulated_" + inverter_id,
        ),
        secondary_y=False,
    )

fig.update_layout(
    title="Neural data",
    xaxis_title="t",
    yaxis_title="Energy (kW)",
    template="plotly_dark",
)

fig.show()
fig.write_html("./output/pv_sim_output.html")
print("Done")
