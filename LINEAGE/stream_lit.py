import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import plotly.graph_objects as go
import math

session = get_active_session()

st.title("Snowflake Lineage Graph (ADMIN_DB.UTIL_SCH)")

days_back = st.slider("Days back", 1, 90, 7)
max_edges = st.number_input("Max edges", min_value=100, max_value=50000, value=5000, step=100)

df = session.sql(f"""
  SELECT SOURCE_OBJECT, TARGET_OBJECT
  FROM ADMIN_DB.UTIL_SCH.LINEAGE_EDGES_LATEST
  WHERE QUERY_START_TIME >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    AND SOURCE_OBJECT IS NOT NULL
    AND TARGET_OBJECT IS NOT NULL
  LIMIT {max_edges}
""").to_pandas()

if df.empty:
    st.warning("No lineage edges found for the selected window.")
    st.stop()

# Build node list
nodes = pd.Index(pd.unique(df[['SOURCE_OBJECT', 'TARGET_OBJECT']].values.ravel('K')))
node_map = {n: i for i, n in enumerate(nodes)}

# Simple circular layout
N = len(nodes)
x = [math.cos(2 * math.pi * i / N) for i in range(N)]
y = [math.sin(2 * math.pi * i / N) for i in range(N)]

# Edges
edge_x, edge_y = [], []
for s, t in df.itertuples(index=False):
    i, j = node_map[s], node_map[t]
    edge_x += [x[i], x[j], None]
    edge_y += [y[i], y[j], None]

fig = go.Figure()
fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode='lines'))
fig.add_trace(
    go.Scatter(
        x=x, y=y,
        mode='markers+text',
        text=nodes.tolist(),
        textposition="top center"
    )
)

st.plotly_chart(fig, use_container_width=True)
st.caption(f"Nodes: {len(nodes)} | Edges: {len(df)}")
