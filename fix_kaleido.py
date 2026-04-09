import re

with open("main.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
skip_next = False
for i, line in enumerate(lines):
    # Skip lines that call write_image with kaleido in plot_oee_time_series
    if "write_image(" in line and "kaleido" in line:
        skip_next = True
        continue
    # Skip the "except Exception: pass" that follows the write_image
    if skip_next and "except Exception: pass" in line:
        skip_next = False
        continue
    skip_next = False
    new_lines.append(line)

with open("main.py", "w", encoding="utf-8") as f:
    f.writelines(new_lines)

removed = len(lines) - len(new_lines)
print(f"Removed {removed} lines (kaleido write_image calls)")
remaining = sum(1 for l in new_lines if "write_image" in l)
print(f"Remaining write_image lines: {remaining}")
