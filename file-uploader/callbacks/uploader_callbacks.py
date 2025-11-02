from dash import dash_table, no_update
import pandas as pd
import base64
import io
from dash import Input, Output, State
import databricks_funcs as df




def register_callbacks(app):

    # ------------------------------------------------------------------------
    # Dependent dropdowns
    # ------------------------------------------------------------------------
    @app.callback(
        Output("schema-dd", "options"),
        Output("schema-dd", "value"),
        Input("catalog-dd", "value"),
    )
    def update_schemas_dd(catalog):
        if not catalog:
            return [], None
        try:
            schemas = df.get_schemas(catalog)
            return ([{"label": s, "value": s} for s in schemas], None)
        except Exception as e:
            print(f"⚠️ Error loading schemas: {e}")
            return [], None


    @app.callback(
        Output("table-dd", "options"),
        Output("table-dd", "value"),
        Input("catalog-dd", "value"),
        Input("schema-dd", "value"),
    )
    def update_tables_dd(catalog, schema):
        if not (catalog and schema):
            return [], None
        try:
            tables = df.get_tables(catalog, schema)
            return ([{"label": t, "value": t} for t in tables], None)
        except Exception as e:
            print(f"⚠️ Error loading tables: {e}")
            return [], None


    # ------------------------------------------------------------------------
    # Merge column dropdown
    # ------------------------------------------------------------------------
    @app.callback(
        Output("merge-id-dd", "options"),
        Output("merge-help", "children"),
        Input("table-dd", "value"),
        State("catalog-dd", "value"),
        State("schema-dd", "value"),
    )
    def update_merge_columns(table, catalog, schema):
        if not (catalog and schema and table):
            return [], "Select a table to load available columns."
        try:
            cols = df.get_columns(catalog, schema, table)
            options = [{"label": c, "value": c} for c in cols]
            msg = f"Found {len(cols)} columns in {catalog}.{schema}.{table}."
            return options, msg
        except Exception as e:
            return [], f"Error loading columns: {e}"


    # ------------------------------------------------------------------------
    # Toggle merge section
    # ------------------------------------------------------------------------
    @app.callback(Output("merge-section", "style"), Input("write-mode", "value"))
    def toggle_merge_section(write_mode):
        return {"display": "block"} if write_mode == "merge" else {"display": "none"}



    # -------------------------------------------------------------------
    # Callback: load CSV into a pandas DataFrame
    # -------------------------------------------------------------------
    @app.callback(
        Output("file-info", "children"),
        Output("uploaded-file-path", "data"),  # ✅ second output to store file path
        Input("upload-data", "contents"),
        State("upload-data", "filename"),
    )
    def load_csv(contents, filename):
        if contents is None:
            return "No file uploaded yet.", None

        try:
            # Decode uploaded CSV
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            pdf = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

            # Save locally and upload to volume
            pdf.to_csv(filename, index=False)
            df.put_file(
                filename=filename,
                localfile_path=filename,
                volume_path="/Volumes/main/chynoweth/demovolume/stg"
            )

            # Construct full path for later use
            uploaded_path = f"/Volumes/main/chynoweth/demovolume/stg/{filename}"
            print(f"✅ Uploaded file to: {uploaded_path}")

            msg = f"📄 File '{filename}' loaded successfully — {pdf.shape[0]} rows, {pdf.shape[1]} columns."
            return msg, uploaded_path

        except Exception as e:
            return f"❌ Error loading file: {e}", None



    # ------------------------------------------------------------------------
    # Execute COPY INTO
    # ------------------------------------------------------------------------
    @app.callback(
        Output("toast-msg", "is_open"),
        Output("toast-msg", "header"),
        Output("toast-msg", "children"),
        Output("toast-msg", "icon"),
        Input("execute-btn", "n_clicks"),
        State("uploaded-file-path", "data"),  
        State("catalog-dd", "value"),
        State("schema-dd", "value"),
        State("table-dd", "value"),
        State("write-mode", "value"),
        State("merge-id-dd", "value"),
        prevent_initial_call=True,
    )
    def execute_write(n_clicks, uploaded_file_path, catalog, schema, table, write_mode, merge_key=None):
        if not uploaded_file_path:
            return True, "Error", "No uploaded file found. Please upload a CSV first.", "danger"
        if not (catalog and schema and table):
            return True, "Error", "Please select a catalog, schema, and table.", "danger"

        print(f"📂 Using uploaded file: {uploaded_file_path}")



        if write_mode in ("append", "overwrite"):
            if write_mode == "overwrite":
                df.truncate_table(table_name=f"{catalog}.{schema}.{table}")
        
            df.append_file(
                file_path=uploaded_file_path,
                target_table=f"{catalog}.{schema}.{table}"
            )
            msg = f"✅ Wrote file into {catalog}.{schema}.{table}"
            return True, "Success", msg, "success"
        
        elif write_mode == 'merge' and merge_key is not None:
            df.merge_file(
                file_path=uploaded_file_path, 
                target_table=f"{catalog}.{schema}.{table}", 
                merge_key=merge_key
                )
            
            msg = f"✅ Wrote file into {catalog}.{schema}.{table}"
            return True, "Success", msg, "success"

        else:
            return True, "Error", "Unsupported write mode operation.", "danger"



    # ------------------------------------------------------------------------
    # Callback: Preview uploaded CSV (first 10 rows)
    # ------------------------------------------------------------------------
    @app.callback(
        Output("preview-alert", "children"),
        Output("preview-table", "children"),
        Input("preview-btn", "n_clicks"),
        State("upload-data", "contents"),
        State("upload-data", "filename"),
        prevent_initial_call=True,
    )
    def preview_uploaded_csv(n_clicks, contents, filename):
        if not contents:
            return "Please upload a CSV file first.", no_update

        try:
            # Decode uploaded file
            content_type, content_string = contents.split(",")
            decoded = base64.b64decode(content_string)
            df_preview = pd.read_csv(io.StringIO(decoded.decode("utf-8")))

            # Take top 10 rows for preview
            preview_df = df_preview.head(10)

            # Convert to a Dash DataTable
            table = dash_table.DataTable(
                data=preview_df.to_dict("records"),
                columns=[{"name": i, "id": i} for i in preview_df.columns],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "5px", "fontFamily": "sans-serif"},
                style_header={"backgroundColor": "#f1f3f4", "fontWeight": "bold"},
                page_action="none",
            )

            msg = f"✅ Previewing '{filename}' — showing first 10 rows.",

            return msg, table

        except Exception as e:
            error_msg = f"Error previewing file: {e}"
            return error_msg, no_update
