import os
import asyncio
from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px
from dateutil import tz

import models


import pandas

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

app = Dash(__name__)


app.layout = html.Div(
    html.Div(
        [
            dcc.Graph(id="live-update-graph"),
            dcc.Interval(
                id="interval-temp", interval=30 * 1000, n_intervals=0  # in milliseconds
            ),
        ]
    )
)



@callback(Output("live-update-graph", "figure"), Input("interval-temp", "n_intervals"))
def update_graph_live(n):
    db_data = loop.run_until_complete(models.get_data())


    data = dict(
        (d.ts.astimezone(tz.gettz('Asia/Bangkok')), d.value) for d in db_data
    )
    records = dict(date=data.keys(), pm_2_5=data.values())

    df = pandas.DataFrame.from_dict(records)
    df["pm_2_5"] = df["pm_2_5"].astype(float)
    fig = px.line(df, x="date", y="pm_2_5", title="PM 2.5 Levels Over Time")
    return fig


if __name__ == "__main__":

    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/environment")
    loop.run_until_complete(models.initialize_beanie(mongodb_uri))

    app.run(debug=True, host="0.0.0.0")