"""Patch schema.md section 3.2 with operative date logic."""
with open("schema.md", "r", encoding="utf-8") as f:
    content = f.read()

old = """## 3.2 Consultas por turno (intenci\u00f3n: **HIST\u00d3RICO**)

- Usar SIEMPRE en conjunto:
  - `dbo.WorkShiftExecutions` (`wse`)
  - `dbo.WorkShiftTemplates` (`wst`)
  - `ind.WorkShiftExecutionSummaries` (`wses`)
- Filtrar **solo turnos cerrados**: `wse.Status = 'closed'`.
- Filtros principales:
  - Por fecha / rango de fechas usando `wse.StartDate`.
  - Por nombre de turno (`wst.Name` = \u201cPrimer Turno\u201d, \u201cSegundo Turno\u201d, \u201cTercer Turno\u201d).
  - Opcionalmente por l\u00ednea (`wses.ProductionLineId` unido a `dbo.ProductionLines`)."""

new = """## 3.2 Consultas por turno (intenci\u00f3n: **HIST\u00d3RICO**)

- Usar SIEMPRE en conjunto:
  - `dbo.WorkShiftExecutions` (`wse`)
  - `dbo.WorkShiftTemplates` (`wst`)
  - `ind.WorkShiftExecutionSummaries` (`wses`)
- Filtrar **solo turnos cerrados**: `wse.Status = 'closed'`.
- **NUNCA filtrar por `wse.StartDate` directo**. Usar siempre la **Fecha Operativa (ShiftBusinessDate)**:

  ```sql
  CASE
    WHEN wst.EndTime < wst.StartTime   -- turno cruza medianoche (ej. Tercer Turno)
      THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
    ELSE CAST(wse.StartDate AS date)   -- turnos normales
  END
  ```

  Esta expresi\u00f3n va **tanto en el `SELECT` (columna `Fecha`) como en el `WHERE`**.
- Por qu\u00e9: el **Tercer Turno** (23:00 \u2192 07:00) cruza la medianoche. Su `StartDate` es el d\u00eda D a las 23:00, pero su `EndDate` es el d\u00eda D+1 a las 07:00. Si el usuario pide datos del d\u00eda D, el Tercer Turno debe incluirse.
- Filtros principales:
  - Por fecha / rango de fechas usando la **Fecha Operativa** calculada arriba.
  - Por nombre de turno (`wst.Name` = \u201cPrimer Turno\u201d, \u201cSegundo Turno\u201d, \u201cTercer Turno\u201d).
  - Opcionalmente por l\u00ednea (`wses.ProductionLineId` unido a `dbo.ProductionLines`)."""

if old in content:
    content = content.replace(old, new, 1)
    with open("schema.md", "w", encoding="utf-8") as f:
        f.write(content)
    print("✅ schema.md actualizado correctamente.")
else:
    print("❌ No se encontró el bloque a reemplazar. Imprimiendo fragmento relevante:")
    idx = content.find("3.2 Consultas")
    print(repr(content[idx:idx+600]))
