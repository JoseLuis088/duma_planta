import os
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Mock the PLOTS_DIR and uuid for the test
import main
main.PLOTS_DIR = "static/plots"

data = [
    ['2026-02-18', 'Primer Turno', 50.0],
    ['2026-02-18', 'Segundo Turno', 45.0],
    ['2026-02-18', 'Tercer Turno', 20.0],
    ['2026-02-19', 'Primer Turno', 72.0],
    ['2026-02-19', 'Segundo Turno', 120.0],
    ['2026-02-19', 'Tercer Turno', 285.0],
]
df = pd.DataFrame(data, columns=['Fecha', 'Turno', 'OEE'])

spec = {
    "chart": "line",
    "x": "Fecha",
    "ys": ["OEE"],
    "title": "Test OEE Auto-Hue"
}

print("Running render_chart_from_df with mock data...")
url = main.render_chart_from_df(df, spec)
print(f"DONE. Chart generated at: {url}")
