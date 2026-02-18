import pandas as pd

df = pd.read_csv("verify_results.csv")
for _, r in df.iterrows():
    print("Turno:", r["Turno"])
    print("  StartDate:     ", r["StartDate"])
    print("  EndDate:       ", r["EndDate"])
    print("  FechaOperativa:", r["FechaOperativa"])
    print("  OEE:           ", r["Oee"])
    print()
