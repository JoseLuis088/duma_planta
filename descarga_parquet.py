from dotenv import load_dotenv
load_dotenv()

from main import load_critical_reads_for_shift, plot_all_critical_timeseries

day = "2026-01-04"
shift = "Primer"

df = load_critical_reads_for_shift(day, shift)

paths = plot_all_critical_timeseries(df, out_dir="static/plots", filename_prefix=f"{day}_{shift}")
print("HTML generados:")
for p in paths:
    print(" -", p)

