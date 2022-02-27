PLOT COLLECTION
FUNNEL PLOT
from plotly import graph_objects as go


fig = go.Figure()
name_list = [x for x in 'ABCDE']
step_list = ["Website visit", "Downloads", "Potential customers", "Requested price", "invoice sent", "Finalized"]
step_values = [10,20,30,40,50]
textinfo = ["value+percent total"]


trace_list =    [
                    {
                        "name":name_list[0],
                        "orientation": "h",
                        "y":step_list,
                        "x": step_values,
                        "textinfo": textinfo
                    },
                    {
                        "name":name_list[1],
                        "orientation": "h",
                        "y":step_list,
                        "x": step_values,
                        "textinfo": textinfo
                    },
                    {
                        "name":name_list[2],
                        "orientation": "h",
                        "y":step_list,
                        "x": step_values,
                        "textinfo": textinfo
                    },
                    {
                        "name":name_list[3],
                        "orientation": "h",
                        "y":step_list,
                        "x": step_values,
                        "textinfo": textinfo
                    }
                ]

for x in trace_list:
    fig.add_trace(go.Funnel(
        name = x['name'],
        orientation=x['orientation'],
        y = list(x['y']),
        x = x['x'],
        textinfo = x['textinfo'][0]))
fig.show()
    
%md ALTERNATIVE PLOT

import plotly.graph_objects as go
import pandas as pd


A ={"nome" : "A", "tamanho" : 12, "parents": "empresas"}
B ={"nome" : "B", "tamanho" : 5, "parents": "empresas"} 
l1 = [A, B]
df = pd.DataFrame(l1)

fig = go.Figure(
    go.Icicle(
        ids = df.nome,
        labels = df.tamanho,
        parents = df.parents,
        root_color="lightgrey",
        tiling = dict(
            orientation='h'
        )
    )
)
fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
fig.show()
#