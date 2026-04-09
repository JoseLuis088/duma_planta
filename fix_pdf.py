import codecs

with codecs.open("main.py", "r", "utf-8") as f:
    content = f.read()

# 1. Quitar acentos que rompen Plotly
content = content.replace('"Desempeño",', '"Desempeno",')
content = content.replace('"Mínimo clase mundial (65%)"', '"Minimo clase mundial (65%)"')

# 2. Modificar la firma de plot_oee_time_series
content = content.replace("def plot_oee_time_series(from_day, daily_data):", "def plot_oee_time_series(from_day, daily_data, export_png=False):")

# 3. Agregar generación de PDF sólo si export_png es True
for i in range(1, 6):
    old_str = f"fig{i}.write_html(os.path.join(out_dir, f{i}_html))"
    new_str = f"fig{i}.write_html(os.path.join(out_dir, f{i}_html))\n    if export_png:\n        try: fig{i}.write_image(os.path.join(out_dir, f{i}_png), engine='kaleido')\n        except Exception: pass"
    content = content.replace(old_str, new_str)

# 4. Usar to_thread para evitar que kaleido bloquee el main loop de FastAPI
content = content.replace("plots = plot_oee_time_series(from_day, daily_data)", "import asyncio\n        plots = await asyncio.to_thread(plot_oee_time_series, from_day, daily_data, payload.get('generate_images', False))")

# 5. Modificar llamadas desde el endpoint de PDF para que envíe "generate_images" = True
content = content.replace(
    'data = await api_oee_day_turn({"from_day": from_day, "to_day": to_day, "shift_name": shift_name})', 
    'data = await api_oee_day_turn({"from_day": from_day, "to_day": to_day, "shift_name": shift_name, "generate_images": True})'
)
content = content.replace(
    'oee_data = await api_oee_day_turn({"from_day": from_day, "to_day": to_day, "shift_name": shift_name})', 
    'oee_data = await api_oee_day_turn({"from_day": from_day, "to_day": to_day, "shift_name": shift_name, "generate_images": True})'
)

with codecs.open("main.py", "w", "utf-8") as f:
    f.write(content)

print("Cambios realizados con exito en main.py")
