from dash import dcc, html
import dash_bootstrap_components as dbc
import databricks_funcs as df


# Header
header = dbc.Row(
            [
                dbc.Col(
                    html.H2("📦 Table Uploader", className="fw-bold text-dark mb-0"),
                    width="auto",
                    className="d-flex align-items-center",
                ),
                dbc.Col(
                    html.Img(
                        src="https://www.invisocorp.com/wp-content/uploads/2018/08/Logo.png",
                        style={
                            "height": "45px",
                            "objectFit": "contain",
                            "backgroundColor": "#0f385b",
                            "borderRadius": "4px",
                            "padding": "4px",
                        },
                    ),
                    width="auto",
                    className="d-flex justify-content-end align-items-center",
                ),
            ],
            justify="between",
            align="center",
            className="mb-4 mt-3 g-0",
        )

# Choose table
table_selection_card = dbc.Card([
            dbc.CardHeader("Step 1: Choose Target Table"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("📁 Catalog", className="fw-semibold"),
                        dcc.Dropdown(
                            id="catalog-dd",
                            options=[{"label": c, "value": c} for c in df.get_catalogs()],
                            placeholder="Select a catalog",
                            searchable=True,
                            clearable=True,
                        ),
                    ], md=4),
                    dbc.Col([
                        html.Label("📂 Schema", className="fw-semibold"),
                        dcc.Dropdown(id="schema-dd", placeholder="Select a schema", searchable=True, clearable=True),
                    ], md=4),
                    dbc.Col([
                        html.Label("🧱 Table", className="fw-semibold"),
                        dcc.Dropdown(id="table-dd", placeholder="Select a table", searchable=True, clearable=True),
                    ], md=4),
                ], className="g-3"),
            ])
        ], className="mb-4 shadow-sm")



# Upload file
upload_file_card = dbc.Card([
            dbc.CardHeader("Step 2: Upload Your File"),
            dbc.CardBody([
                html.Label("Upload File (.csv)", className="fw-semibold"),
                dcc.Upload(
                    id="upload-data",
                    children=html.Div([
                        "Drag and drop or ", html.A("browse for a file", className="text-decoration-underline fw-semibold")
                    ]),
                    style={
                        'width': '100%',
                        'height': '80px',
                        'lineHeight': '80px',
                        'borderWidth': '1px',
                        'borderStyle': 'dashed',
                        'borderRadius': '8px',
                        'textAlign': 'center',
                        'backgroundColor': '#fafafa'
                    },
                    multiple=False,
                ),
                html.Div(id="file-info", style={"marginTop": "20px"}),
                html.Small("💡 Note: CSV files only for now.", className="text-muted")
            ])
        ], className="mb-4 shadow-sm")

# Write options
write_options_card = dbc.Card([
            dbc.CardHeader("Step 3: Choose Write Mode"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("Write Mode", className="fw-semibold"),
                        dcc.RadioItems(
                            id="write-mode",
                            options=[
                                {"label": " Overwrite table", "value": "overwrite"},
                                {"label": " Append to table", "value": "append"},
                                {"label": " Merge into table (UPSERT)", "value": "merge"},
                            ],
                            value="append",
                            labelStyle={'display': 'block', 'marginTop': '0.25rem'}
                        ),
                        dbc.Tooltip("Select how data should be written to the table.",
                                    target="write-mode", placement="right"),
                    ], md=4),
                    dbc.Col([
                        html.Div(id="merge-section", children=[
                            html.Label("Merge Key (ID column)", className="fw-semibold"),
                            dcc.Dropdown(id="merge-id-dd",
                                         placeholder="Select ID column for MERGE",
                                         clearable=True),
                            html.Div(id="merge-help", className="text-muted mt-2",
                                     style={"fontSize": "0.85rem"})
                        ])
                    ], md=8),
                ])
            ])
        ], className="mb-4 shadow-sm")

# Preview and actions
action_preview_card = dbc.Card([
            dbc.CardHeader("Step 4: Preview and Execute"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Button("👁 Preview Upload", id="preview-btn", color="secondary", className="mt-2"),
                        dbc.Button("🚀 Execute Write", id="execute-btn", color="primary", className="mt-2 ms-2"),
                    ], md=12),
                ]),
                html.Div(id="preview-alert", className="mt-3"),
                html.Hr(),
                html.H5("Preview (first 10 rows)", className="fw-semibold"),
                html.Div(id="preview-table", style={'overflowX': 'auto'}),
            ])
        ], className="mb-4 shadow-sm")



# Toast notifications
toast_notifications = dbc.Toast(
            id="toast-msg",
            header="Status",
            is_open=False,
            dismissable=True,
            icon="primary",
            duration=4000,
            style={"position": "fixed", "bottom": 20, "right": 20, "width": 350},
        )



# Hidden stores
uploaded_dataframe = dcc.Store(id="uploaded-df", storage_type="memory")
uploaded_file_path = dcc.Store(id="uploaded-file-path", storage_type="memory")
