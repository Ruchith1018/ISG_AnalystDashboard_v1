import dash
from dash import html, Input, Output, State, dcc
import dash_ag_grid as dag
import pandas as pd
from sqlalchemy import create_engine, text

import os
from dotenv import load_dotenv

load_dotenv()

# ------------------ DB CONFIG ------------------

ps_host = os.environ["PS_HOST"]
ps_port = int(os.environ["PS_PORT"])
ps_user = os.environ["PS_USER"]
ps_password = os.environ["PS_PASSWORD"]
ps_dbname = os.environ["PS_DBNAME"]

engine = create_engine(
    f"postgresql+psycopg2://{ps_user}:{ps_password}@{ps_host}:{ps_port}/{ps_dbname}",
    pool_pre_ping=True
)

TABLE_NAME = "news"
UNIQUE_COL = "news_id"

# ------------------ HELPERS ------------------
def clean_value(x):
    """Convert NaN-like values to None"""
    if isinstance(x, pd.Series):
        if x.empty:
            return None
        x = x.iloc[0]

    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if isinstance(x, str) and x.strip().lower() in ("nan", "<na>", ""):
        return None
    return x

def normalize_value(v):
    """Normalize types to match DB types"""
    if pd.isna(v):
        return None
    if isinstance(v, str):
        # Convert booleans
        if v.lower() == "true":
            return True
        if v.lower() == "false":
            return False
        # Convert integers if numeric string
        try:
            if "." in v:
                return float(v)
            else:
                return int(v)
        except:
            return v
    return v

def rows_differ(row1, row2, cols):
    for col in cols:
        v1 = normalize_value(row1.get(col))
        v2 = normalize_value(row2.get(col))
        if v1 != v2:
            return True
    return False



# ------------------ LOAD DATA ------------------
def load_news():
    return pd.read_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 500", engine)

def load_categories():
    return pd.read_sql(
        "SELECT category_id, category_name, risk_level, risk_rating FROM categories",
        engine
    )

df = load_news()
cat_df = load_categories()

# ------------------ CATEGORY LOOKUP ------------------
CATEGORY_OPTIONS = sorted(cat_df["category_name"].dropna().tolist())

CAT_LOOKUP = {
    row["category_name"]: {
        "risk_level": int(row["risk_level"]) if pd.notna(row["risk_level"]) else None,
        "risk_rating": row["risk_rating"],
        "category_id": str(row["category_id"]),
    }
    for _, row in cat_df.iterrows()
}

# ------------------ COLUMN ORDER ------------------
COLUMN_ORDER = [
    "company_name","date","headline","content","url",
    "is_duplicate","analyst_dup","qc_tags","analyst_tag",
    "category_name","analyst_cat","risk_rating","risk_level",
    "analyst_risk_level","analyst_approval","analyst_remark",
    "analyst_cat_id","filtered_content","processed",
    "news_id","company_id","ads_removed_flag","removed_text",
    "dup_group_id","cat_reason","tag_reason","category_id",
    "orbis_id","locations","source","ingestion_date",
    "created_at","lang","cutoff_time","blogpost_links",
    "valid","url_status","url_status_code"
]

df = df[[c for c in COLUMN_ORDER if c in df.columns]]

# ------------------ EDITABLE COLS ------------------
EDITABLE_COLS = {
    "analyst_dup",
    "analyst_cat",
    "analyst_tag",
    "analyst_risk_level",
    "analyst_approval",
    "analyst_remark",
}

# ------------------ COLUMN DEFINITIONS ------------------
column_defs = []

for col in df.columns:
    col_def = {
        "field": col,
        "editable": col in EDITABLE_COLS,
        "filter": True,
        "sortable": True,
        "resizable": True,
    }

    if col in EDITABLE_COLS:
        col_def["cellStyle"] = {
            "backgroundColor": "#fff3c4",
            "borderLeft": "3px solid #f59e0b",
        }

    if col in ("analyst_dup", "analyst_approval"):
        col_def.update({
            "cellEditor": "agSelectCellEditor",
            "cellEditorParams": {"values": [True, False]},
            "cellStyle": {
                "styleConditions": [
                    {"condition": "params.value === true",
                     "style": {"backgroundColor": "#bbf7d0"}},
                    {"condition": "params.value === false",
                     "style": {"backgroundColor": "#fecaca"}},
                    {"condition": "params.value == null",
                     "style": {"backgroundColor": "#fff3c4",
                               "borderLeft": "3px solid #f59e0b"}},
                ]
            }
        })

    if col == "analyst_cat":
        col_def.update({
            "cellEditor": "agSelectCellEditor",
            "cellEditorParams": {"values": CATEGORY_OPTIONS},
            "valueSetter": {
                "function": """
                function(params) {
                    const cat = params.newValue;
                    if (!cat || !window.catLookup[cat]) return false;

                    params.data.analyst_cat = cat;
                    params.data.analyst_risk_level = window.catLookup[cat].risk_level;
                    params.data.risk_rating = window.catLookup[cat].risk_rating;
                    params.data.analyst_cat_id = window.catLookup[cat].category_id;
                    return true;
                }
                """
            }
        })

    column_defs.append(col_def)

# ------------------ DASH APP ------------------
app = dash.Dash(__name__)

app.layout = html.Div(
    style={"padding": "15px"},
    children=[
        html.H3("📰 News Analyst QC Dashboard"),

        dag.AgGrid(
            id="news-grid",
            rowData=df.to_dict("records"),
            columnDefs=column_defs,
            dashGridOptions={
                "pagination": True,
                "paginationPageSize": 25,
                "animateRows": True,
                "onGridReady": {
                    "function": f"function(){{ window.catLookup = {CAT_LOOKUP}; }}"
                },
                "getRowStyle": {
                    "styleConditions": [
                        {"condition": "params.node.rowIndex % 2 === 0",
                         "style": {"backgroundColor": "#e5e7eb"}},
                        {"condition": "params.node.rowIndex % 2 === 1",
                         "style": {"backgroundColor": "#ffffff"}},
                    ]
                },
            },
            style={"height": "78vh", "width": "100%"},
        ),

        html.Br(),

        dcc.Loading(
            id="saving-loader",
            type="circle",
            fullscreen=True,
            children=html.Button("💾 Save Changes", id="save-btn", style={"fontSize": "16px"})
        ),

        # -------- POPUP --------
        html.Div(
            id="popup",
            style={
                "display": "none",
                "position": "fixed",
                "top": "50%",
                "left": "50%",
                "transform": "translate(-50%, -50%)",
                "backgroundColor": "white",
                "padding": "30px",
                "borderRadius": "10px",
                "boxShadow": "0 10px 40px rgba(0,0,0,0.4)",
                "zIndex": 9999,
                "textAlign": "center",
            },
            children=[
                html.Div(id="popup-msg", style={"fontSize": "20px", "marginBottom": "20px"}),
                html.Button("OK", id="close-popup", style={"fontSize": "16px"})
            ]
        )
    ]
)

# ------------------ SAVE CALLBACK ------------------
@app.callback(
    Output("popup", "style"),
    Output("popup-msg", "children"),
    Input("save-btn", "n_clicks"),
    Input("close-popup", "n_clicks"),
    State("news-grid", "rowData"),
    prevent_initial_call=True
)
def save_changes(save_clicks, close_clicks, rows):

    if dash.callback_context.triggered_id == "close-popup":
        return {"display": "none"}, ""

    new_df = pd.DataFrame(rows).set_index(UNIQUE_COL)

    base_df = pd.read_sql(
        """
        SELECT news_id, analyst_dup, analyst_cat, analyst_cat_id,
               analyst_risk_level, analyst_tag,
               analyst_approval, analyst_remark
        FROM news
        """,
        engine
    ).set_index(UNIQUE_COL)

    editable_cols = [
        "analyst_dup","analyst_cat","analyst_cat_id",
        "analyst_risk_level","analyst_tag",
        "analyst_approval","analyst_remark"
    ]

    # ---- robust diff ----
    diff_ids = []
    for idx, row in new_df.iterrows():
        if idx not in base_df.index:
            diff_ids.append(idx)
        else:
            old_row = base_df.loc[idx].to_dict()
            if rows_differ(row, old_row, editable_cols):
                diff_ids.append(idx)

    if not diff_ids:
        return {"display": "block"}, "ℹ️ No changes to save"

    # ---- save only changed rows ----
    with engine.begin() as conn:
        for news_id in diff_ids:
            row = new_df.loc[[news_id]].iloc[0]
            data = {c: clean_value(row[c]) for c in editable_cols}
            data["news_id"] = news_id

            if data["analyst_risk_level"] is not None:
                data["analyst_risk_level"] = int(data["analyst_risk_level"])

            conn.execute(
                text("""
                    UPDATE news SET
                        analyst_dup = :analyst_dup,
                        analyst_cat = :analyst_cat,
                        analyst_cat_id = :analyst_cat_id,
                        analyst_risk_level = :analyst_risk_level,
                        analyst_tag = :analyst_tag,
                        analyst_approval = :analyst_approval,
                        analyst_remark = :analyst_remark
                    WHERE news_id = :news_id
                """),
                data
            )

    return {"display": "block"}, f"✅ Saved {len(diff_ids)} records"

# ------------------ RUN ------------------
if __name__ == "__main__":
    app.run(debug=True)
