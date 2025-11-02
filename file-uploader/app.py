from dash import Dash
import dash_bootstrap_components as dbc
from layouts.index import *

# ------------------------------------------------------------------------
# Initialize the Dash app
# ------------------------------------------------------------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Databricks Excel Uploader"


# ------------------------------------------------------------------------
# Layout
# ------------------------------------------------------------------------
app.layout = dbc.Container(
    [
        header,
        table_selection_card,
        upload_file_card,
        write_options_card,
        action_preview_card,
        toast_notifications,
        uploaded_dataframe,
        uploaded_file_path,
    ],
    fluid=True,
    className="p-4",
    style={"backgroundColor": "#eff8ff"},
)

from callbacks.uploader_callbacks import register_callbacks

register_callbacks(app)



# ------------------------------------------------------------------------
# Run app
# ------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=8050)
