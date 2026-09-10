# Manual de Usuario Sidón Industrial — Base de Conocimiento para IA

> Documento derivado del Manual de Usuario Sidón Industrial (versión 3.0, agosto 2026), reestructurado para consumo por un agente de IA / sistema RAG (Azure AI Search). Cada sección es autocontenida: repite el nombre de los módulos, pantallas y botones de Sidón Industrial en lugar de usar pronombres ambiguos.

## Perfiles del sistema Sidón Industrial

Sidón Industrial reconoce cinco perfiles de usuario: **Operador**, **Supervisor**, **Coordinador**, **Gerente** y **Administrador**. Lo que cada persona ve en Sidón Industrial depende de su perfil: el sistema oculta automáticamente las opciones para las que un perfil no tiene permiso. Si una opción de este manual no aparece en la pantalla de un usuario, es porque su perfil no la tiene autorizada.

## Cómo leer este manual (convenciones)

- Los nombres de botones y campos de Sidón Industrial aparecen en **negritas**, por ejemplo: **Ingreso MP**.
- Las rutas de navegación dentro del menú de Sidón Industrial aparecen en *cursivas*, por ejemplo: *Dashboards › Líneas de producción*.
- Los recuadros de color siguen una convención fija: **verde = informativo**, **ámbar = consejo**, **rojo = advertencia de algo que puede salir mal**.

---

# Capítulo 1 · Antes de empezar

## 1.0 Introducción al sistema Sidón Industrial

Sidón Industrial es una solución diseñada para monitorear, analizar y optimizar en tiempo real los procesos de producción dentro de la planta. Sidón Industrial sigue la producción de la planta minuto a minuto, recoge de forma automática lo que ocurre en cada línea de producción (cuánto se produce, cuándo se detiene, etc.) y convierte esos datos en indicadores que el personal puede consultar y sobre los que puede actuar.

El objetivo principal de Sidón Industrial es mejorar la eficiencia operativa, reducir cuellos de botella y garantizar la calidad del producto final, mediante el uso de sensores, dashboards y herramientas de análisis integradas.

**Principales beneficios de Sidón Industrial:**

- Monitoreo en tiempo real de variables críticas de producción.
- Cálculo automático del OEE (Eficiencia Global de Equipos).
- Gestión de costos y merma, con trazabilidad y reportes históricos.
- Alertas y notificaciones para anticipar incidencias y facilitar la toma de decisiones.
- Integración con SAP, que permite obtener información precisa y actualizada.
- Escalabilidad: Sidón Industrial puede crecer y adaptarse a otras líneas de producción en el futuro.

### Preguntas frecuentes — Introducción al sistema Sidón Industrial

**¿Qué mide Sidón Industrial exactamente?**
Sidón Industrial mide en tiempo real qué ocurre en cada línea de producción (producción, paros, variables de proceso) y convierte esos datos en indicadores como el OEE.

**¿Sidón Industrial se conecta con SAP?**
Sí. Sidón Industrial se integra con SAP para obtener información de productos, materiales y órdenes de producción de forma automática.

## 1.1 ¿Qué es el OEE en Sidón Industrial?

### Introducción al OEE

El OEE (Overall Equipment Effectiveness / Eficacia Global de los Equipos) es el indicador estándar de la industria manufacturera para medir qué tan bien está aprovechando una línea de producción su capacidad potencial dentro de Sidón Industrial. El OEE responde a la pregunta: "De todo el tiempo que la línea podría estar produciendo, ¿cuánto está produciendo bien, sin paros y a la velocidad correcta?"

El OEE se expresa como un porcentaje y se compone de tres factores calculados por Sidón Industrial: **Disponibilidad**, **Desempeño** y **Calidad**. Cada factor del OEE mide una fuente distinta de pérdida productiva.

### Los tres indicadores del OEE

**Disponibilidad (D).** La Disponibilidad mide el tiempo que la línea estuvo realmente operando en comparación con el tiempo que estaba planeada para producir. La Disponibilidad captura las pérdidas por paros no programados, fallas de equipo, cambios de formato y ajustes.

Fórmula de Disponibilidad: **Disponibilidad = (Tiempo de Operación / Tiempo Planeado de Producción) × 100**

| Concepto | Descripción |
|---|---|
| Tiempo Planeado de Producción | Tiempo programado del turno menos paros programados (descansos, limpiezas, etc.) |
| Tiempo de Operación | Tiempo planeado menos el tiempo de paros no programados |

**Ejemplo de cálculo de Disponibilidad:** si el turno es de 8 horas (480 min), se descontaron 30 min de limpieza (Tiempo Planeado de Producción = 450 min) y hubo 45 min de paro por falla de equipo (Tiempo de Operación = 405 min), entonces Disponibilidad = (405 / 450) × 100 = **90%**.

**Desempeño (P).** El Desempeño mide si la línea estuvo corriendo a su velocidad ideal durante el tiempo que sí estuvo operando. El Desempeño captura las pérdidas por velocidad reducida, microparos o ciclos lentos.

Fórmula de Desempeño: **Desempeño = (Producción Real / Producción Teórica Ideal) × 100**

**Ejemplo de cálculo de Desempeño:** si en 405 minutos de operación la línea debería haber producido 1,215 kg (a 3 kg/min) pero solo produjo 1,093 kg, Desempeño = (1,093 / 1,215) × 100 = **90%**.

**Calidad (C).** La Calidad mide la proporción de producto que cumple con los estándares de calidad a la primera pasada, sin retrabajos ni decomisos. La Calidad captura las pérdidas por producto no conforme, merma, reprocesos y arranques defectuosos.

Fórmula de Calidad: **Calidad = (Producto Conforme / Producción Total) × 100**

**Ejemplo de cálculo de Calidad:** de 1,093 kg producidos, 43 kg fueron decomisados o reprocesados (Producto Conforme = 1,050 kg), Calidad = (1,050 / 1,093) × 100 = **96%**.

**Cálculo del OEE final.** El OEE es el producto de los tres indicadores: **OEE = Disponibilidad × Desempeño × Calidad**. Ejemplo: OEE = 90% × 90% × 96% = **77.8%**.

### Referencia de benchmarks mundiales de OEE

| Nivel | OEE | Interpretación |
|---|---|---|
| Clase Mundial | ≥ 85% | Operación de alto rendimiento |
| Típico en la industria | 60% – 85% | Hay áreas de mejora identificables |
| Por debajo del promedio | < 60% | Pérdidas significativas, requiere atención urgente |

> **Nota importante sobre el OEE:** el OEE de Sidón Industrial no penaliza los paros programados (limpiezas, cambios de turno, mantenimientos programados). El OEE solo evalúa lo que ocurre dentro del Tiempo Planeado de Producción.

### Las Seis Grandes Pérdidas del OEE

El OEE fue diseñado para atacar las Seis Grandes Pérdidas de la manufactura, agrupadas por indicador del OEE:

| Indicador del OEE | Pérdida |
|---|---|
| Disponibilidad | 1. Fallas y averías de equipo |
| Disponibilidad | 2. Ajustes y cambios de formato (setup) |
| Desempeño | 3. Microparos e inactividad |
| Desempeño | 4. Velocidad reducida |
| Calidad | 5. Defectos y retrabajos en producción |
| Calidad | 6. Pérdidas por arranque |

### Preguntas frecuentes — OEE en Sidón Industrial

**¿Cómo se calcula el OEE en Sidón Industrial?**
El OEE se calcula multiplicando los tres indicadores que Sidón Industrial reporta: Disponibilidad × Desempeño × Calidad.

**¿Los paros programados bajan el OEE?**
No. Los paros programados no penalizan la Disponibilidad ni el OEE en Sidón Industrial; solo los paros no programados afectan el indicador.

**¿Qué se considera un buen OEE?**
Según el benchmark mundial usado por Sidón Industrial, un OEE de 85% o más se considera Clase Mundial; entre 60% y 85% es típico de la industria; por debajo de 60% requiere atención urgente.

## 1.2 Perfiles de usuario en Sidón Industrial

Lo que cada persona ve en Sidón Industrial depende de su perfil de usuario. El sistema oculta automáticamente las opciones para las que un perfil no tiene permiso: si un apartado de este manual no aparece en la pantalla de un usuario, es porque su perfil no lo incluye.

**Uso principal de cada perfil en Sidón Industrial:**

| Perfil | Uso principal en Sidón Industrial |
|---|---|
| Operador | Monitoreo de su línea, ingreso de materia prima, registro de desvíos y controles de calidad, clasificación de paros. |
| Supervisor | Todo lo del perfil Operador, más histórico de turnos, resumen de planta y bitácoras de consulta. |
| Coordinador | Seguimiento de varias líneas, programación de paros y análisis de desvíos. |
| Gerente | Tableros de resumen e impacto en costos. |
| Administrador | Catálogos, consultas exportables a Excel y análisis de costo y merma. |

A lo largo de este manual, cada apartado indica con la marca **Perfil** qué perfil de Sidón Industrial lo utiliza normalmente.

> **Nota de mantenimiento del manual:** este listado de perfiles refleja los cinco perfiles reales de Sidón Industrial (Operador, Supervisor, Coordinador, Gerente, Administrador). Hay una revisión pendiente en el manual origen para corregir menciones a perfiles que no existen en el sistema (por ejemplo "administrativo", "gerencia", "mantenimiento" o "calidad" usados como perfil en otras secciones).

### Preguntas frecuentes — Perfiles de usuario en Sidón Industrial

**¿Cuáles son los perfiles que existen en Sidón Industrial?**
Los cinco perfiles reales de Sidón Industrial son Operador, Supervisor, Coordinador, Gerente y Administrador.

**No veo una opción del menú que aparece en este manual, ¿qué hago?**
Su perfil de Sidón Industrial no la tiene autorizada. Solicítela a su administrador indicando la sección y la pantalla de Sidón Industrial donde falta la opción.

---

# Capítulo 2 · Primeros pasos en Sidón Industrial

## 2.1 Entrar al sistema Sidón Industrial

**Perfil:** todos los perfiles

Pasos para entrar al sistema Sidón Industrial:

1. Abra el navegador y escriba la dirección de Sidón Industrial que le proporcionó su administrador.
2. Escriba su correo y su contraseña en la pantalla de inicio de sesión de Sidón Industrial.
3. Pulse **Iniciar sesión**. Entrará directamente al tablero principal de Sidón Industrial.
4. Si es la primera vez que inicia sesión en Sidón Industrial, su contraseña temporal será *TemporalPassword#1*.
5. Sidón Industrial le solicitará hacer cambio de contraseña la primera vez que inicie sesión.
6. Si extravía u olvida su contraseña de Sidón Industrial, contacte a su administrador, quien podrá restablecer la contraseña inicial.

### Preguntas frecuentes — Entrar al sistema Sidón Industrial

**¿Cuál es la contraseña temporal la primera vez que entro a Sidón Industrial?**
La contraseña temporal de Sidón Industrial es *TemporalPassword#1*. Sidón Industrial pedirá cambiarla en el primer inicio de sesión.

**Olvidé mi contraseña de Sidón Industrial, ¿qué hago?**
Contacte a su administrador de Sidón Industrial; él puede restablecer la contraseña inicial de su cuenta.

## 2.2 El menú lateral de Sidón Industrial

**Perfil:** todos los perfiles

La franja verde de la izquierda en Sidón Industrial es el menú lateral. El menú lateral está contraído por defecto y solo muestra íconos de sección; al acercar el puntero sobre el menú lateral, este se despliega y muestra los nombres completos de cada sección. Cada sección del menú lateral agrupa una familia de pantallas de Sidón Industrial.

**Secciones del menú lateral de Sidón Industrial:**

| Sección del menú | Qué contiene |
|---|---|
| Duma | Reportes asistidos por inteligencia artificial y agente conversacional. |
| Dashboards | Dispositivos, reporte BI, líneas de producción, resumen de planta y desvíos. |
| Catálogos industriales | Productos, líneas, paros programados, controles de calidad, materiales y taras. |
| Usuarios | Alta y administración de personas. |
| Sense | Dispositivos, sensores, hubs, fabricantes y modelos. |
| Alertamiento | Alertas activas, histórico y configuración. |
| Tenant | Sucursales y sub-sucursales. |
| Consultas | Variables históricas y bitácoras de paros, calidad, metales y etiquetado. |
| Costo y merma | Desvíos, impacto en costos y bitácora de ingresos de materia prima. |

El menú lateral de Sidón Industrial no es igual para todos los usuarios: solo aparecen las secciones del menú lateral que el perfil del usuario tiene autorizadas.

### Preguntas frecuentes — Menú lateral de Sidón Industrial

**¿Por qué no veo todas las secciones del menú lateral de Sidón Industrial?**
El menú lateral de Sidón Industrial solo muestra las secciones que el perfil del usuario tiene autorizadas. Si falta una sección, consúltelo con su administrador de Sidón Industrial.

**¿Cómo despliego los nombres completos del menú lateral?**
Acerque el puntero del mouse sobre la franja verde del menú lateral de Sidón Industrial; el menú lateral se despliega mostrando los nombres completos de cada sección.

## 2.3 Ajustes: tema, idioma y pantalla completa en Sidón Industrial

**Perfil:** todos los perfiles

- **Tema claro u oscuro:** pulse el ícono de sol, ubicado arriba del todo en el menú lateral de Sidón Industrial. Sidón Industrial recuerda la preferencia de tema del usuario.
- **Idioma:** pulse el botón redondo **ES** de la esquina inferior derecha de Sidón Industrial.
- **Pantalla completa:** pulse el logotipo de SIDÓN. La pantalla completa es muy útil en los monitores de piso de producción.

### Preguntas frecuentes — Ajustes de Sidón Industrial

**¿Cómo cambio el tema claro u oscuro en Sidón Industrial?**
Pulse el ícono de sol arriba del menú lateral de Sidón Industrial. Sidón Industrial recordará la preferencia elegida.

**¿Cómo pongo Sidón Industrial en pantalla completa?**
Pulse el logotipo de SIDÓN dentro de Sidón Industrial para activar la pantalla completa.

## 2.4 Cerrar sesión en Sidón Industrial

**Perfil:** todos los perfiles

Para cerrar sesión en Sidón Industrial, pulse el ícono de encendido al final del menú lateral y confirme la acción.

### Preguntas frecuentes — Cerrar sesión en Sidón Industrial

**¿Dónde está el botón para cerrar sesión en Sidón Industrial?**
El botón para cerrar sesión de Sidón Industrial es el ícono de encendido, ubicado al final del menú lateral.

# Capítulo 3 · Tableros de seguimiento en Sidón Industrial

## 3.1 Resumen de líneas en Sidón Industrial

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *Dashboards › Líneas de producción*

La pantalla Resumen de líneas es la puerta de entrada a la operación en Sidón Industrial. Resumen de líneas muestra una tarjeta por cada línea que está produciendo en la sucursal seleccionada, con el estado de la línea, el avance de la línea y los indicadores de la línea.

Cómo se usa la pantalla Resumen de líneas de Sidón Industrial:

1. Elija la sucursal en el desplegable superior de la pantalla Resumen de líneas. Las tarjetas de Resumen de líneas se actualizan solas.
2. Pulse la estrella de una tarjeta de Resumen de líneas para marcarla como favorita; Sidón Industrial recordará esa preferencia la próxima vez que el usuario entre a Resumen de líneas.
3. Pulse sobre una tarjeta de Resumen de líneas para abrir el detalle de esa línea de producción.

> **Actualización automática.** La pantalla Resumen de líneas de Sidón Industrial se refresca sola cada diez minutos. No hace falta recargar el navegador.

### Preguntas frecuentes — Resumen de líneas en Sidón Industrial

**¿Cada cuánto se actualiza la pantalla Resumen de líneas?**
La pantalla Resumen de líneas de Sidón Industrial se actualiza sola cada diez minutos.

**¿Cómo marco una línea como favorita en Resumen de líneas?**
Pulse la estrella de la tarjeta de esa línea en la pantalla Resumen de líneas de Sidón Industrial; Sidón Industrial recordará la preferencia.

## 3.2 Monitoreo de línea en Sidón Industrial

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *Resumen de líneas › pulsar una línea › pestaña Monitoreo*

La pantalla Monitoreo de línea es la pantalla más importante para el personal de piso en Sidón Industrial. Monitoreo de línea reúne en una sola vista el estado de la línea, el avance de la orden de producción, el avance del turno, los indicadores de OEE y la línea de tiempo de la jornada.

**Zonas de la pantalla Monitoreo de línea en Sidón Industrial:**

| Zona de Monitoreo de línea | Contenido |
|---|---|
| Orden en progreso | Número de orden, producto, hora de inicio, producción planeada y real del día, y la barra de avance. |
| Velocidades | Velocidad esperada frente a la real, en kg/h, con el porcentaje de desviación en verde o rojo. También el tiempo estimado y real para cerrar la orden. |
| Turno | Meta y producción real del turno, con su porcentaje. Botones para moverse entre turnos. |
| Estado de la línea | Disponible / Inactiva / En paro, y cuánto tiempo lleva así la línea. |
| Tiempos | Tiempo natural transcurrido, tiempo productivo y tiempo no productivo, separando lo programado de lo no programado. |
| Indicadores | Cuatro donas: OEE, Disponibilidad, Desempeño y Producto Conforme. |
| Línea de tiempo | La jornada hora por hora, dividida en bloques de cinco minutos y coloreada según lo que ocurrió en cada bloque. |

**Moverse entre turnos en Monitoreo de línea:** el botón **Ver histórico** redirige al último turno cerrado de la línea (ver sección 3.5 Histórico de turnos de Sidón Industrial).

### Leer la línea de tiempo en Monitoreo de línea

Cada color de la línea de tiempo de Sidón Industrial indica qué estaba haciendo la línea en ese minuto:

| Color en la línea de tiempo | Significado |
|---|---|
| Verde | La línea produce con normalidad. |
| Amarillo | La línea produce por debajo del 80% de lo esperado. |
| Rojo | Paro no programado sin clasificar. Requiere que alguien indique el motivo del paro. |
| Rojo intenso | Paro no programado ya clasificado. |
| Gris | Paro programado. |

Sobre la barra de la línea de tiempo aparecen íconos que marcan sucesos: paros, controles de calidad, cambios de orden y detecciones de metal. Pulsando un ícono de la línea de tiempo se abre el detalle correspondiente en Sidón Industrial.

> **No se puede clasificar un paro en curso.** Mientras la línea siga detenida, Sidón Industrial avisa "No puede clasificar un Paro No Programado mientras está en ejecución". Espere a que la línea arranque y clasifique el paro después (ver sección 5.3 Clasificar un paro).

**Botones de acción de Monitoreo de línea:** abajo a la derecha de Monitoreo de línea hay tres botones que solo aparecen si el perfil del usuario tiene permiso:

- **Desvío MP** — registrar material desviado o mermado (ver sección 5.1 Desvío de materia prima).
- **Control de Calidad** — levantar un control no planeado (ver sección 5.2 Control de calidad).
- **Ingreso MP** — capturar materia prima (ver Capítulo 4 Ingreso de materia prima). El botón Ingreso MP no aparece en teléfonos.

### Preguntas frecuentes — Monitoreo de línea en Sidón Industrial

**¿Por qué no puedo clasificar un paro en Monitoreo de línea?**
Sidón Industrial no permite clasificar un paro no programado mientras la línea sigue detenida. Debe esperar a que la línea arranque de nuevo.

**¿Qué significa el color rojo en la línea de tiempo de Monitoreo de línea?**
El color rojo en la línea de tiempo de Sidón Industrial significa un paro no programado que todavía no ha sido clasificado.

**¿Dónde está el botón Ingreso MP en Sidón Industrial?**
El botón Ingreso MP está en la parte inferior derecha de la pantalla Monitoreo de línea, pero no aparece en la versión de teléfono de Sidón Industrial.

## 3.3 Flujo de línea en Sidón Industrial

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *detalle de línea › pestaña Flujo de línea*

La pantalla Flujo de línea presenta los equipos de la línea en su orden de proceso, con las lecturas de sus sensores. Flujo de línea sirve para localizar de un vistazo qué equipo de la línea está fuera de rango. El color del recuadro de cada equipo en Flujo de línea indica el estado del equipo: activo, inactivo o en alerta.

### Preguntas frecuentes — Flujo de línea en Sidón Industrial

**¿Para qué sirve la pantalla Flujo de línea en Sidón Industrial?**
Flujo de línea sirve para ver de un vistazo qué equipo de la línea está fuera de rango, según el color del recuadro de cada equipo.

## 3.4 Variables de control en Sidón Industrial

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *detalle de línea › pestaña Variables de control*

La pantalla Variables de control de Sidón Industrial muestra, en gráficas, cómo se han comportado durante el turno las variables de proceso: temperaturas, presiones, velocidades de banda y demás variables críticas de la línea.

Cómo se usa la pantalla Variables de control:

1. Al abrir Variables de control, Sidón Industrial muestra por defecto las variables críticas de la línea.
2. Para ver otras variables, use los filtros de **Equipos** y **Sensores** en Variables de control. Puede marcar varios equipos y sensores a la vez.
3. El botón para ver todas las variables alterna entre las variables críticas y el conjunto completo. Habilite la opción **Ocultar todas las variables** para ver todas las variables de la línea en Variables de control.
4. Desde cada gráfica de Variables de control puede saltar al histórico de esa variable: dé clic sobre el nombre de la variable para abrir la consulta ya filtrada (ver sección 8.2 Histórico de variables).

> **Consejo.** Al marcar varios equipos seguidos en los filtros de Variables de control, espere un momento: Sidón Industrial agrupa los cambios y hace una sola consulta en vez de una por clic.

### Alertamiento por WhatsApp en Sidón Industrial

Cuando una variable crítica de Variables de control sale de su rango normal, Sidón Industrial aplica un periodo de gracia antes de notificar. Si la variable no se restablece dentro de ese periodo de gracia, Sidón Industrial dispara una alerta por WhatsApp que escala automáticamente si nadie resuelve la causa.

Esquema de escalamiento de Alertamiento por WhatsApp en Sidón Industrial:

1. La variable sale de su rango normal. Inicia un periodo de gracia de 15 minutos.
2. Si la variable no regresa a su rango dentro de esos 15 minutos, Sidón Industrial dispara la primera alerta por WhatsApp al perfil Supervisor.
3. Inicia un nuevo periodo de gracia de 1 hora. Si la variable sigue fuera de rango al terminar ese periodo de gracia de 1 hora, la alerta escala: se genera de nuevo para el perfil Supervisor y además se envía al perfil Coordinador.
4. Se repite la misma lógica para el siguiente nivel: si pasa otra hora sin que la variable se restablezca, la alerta escala al perfil Gerente. El esquema de escalamiento de Alertamiento por WhatsApp se detiene en el perfil Gerente; no hay un nivel posterior.

> Las alertas de Alertamiento por WhatsApp no se "atienden" con un botón: se resuelven solas. En cuanto la variable regresa a su rango normal, la alerta se apaga automáticamente y queda registrada como historial.

**Alertamiento por WhatsApp se suspende en paros programados.** Todo el Alertamiento por WhatsApp se suspende mientras la línea está en un Paro Programado, siempre y cuando ese paro haya sido dado de alta previamente en el catálogo de Paros programados (ver Capítulo 7 Paros programados).

**Excepciones al envío de Alertamiento por WhatsApp.** Hay dos casos donde un usuario no recibe notificaciones de Alertamiento por WhatsApp, aunque su perfil sí aplique al esquema de escalamiento:

1. **Número pivote.** El catálogo de usuarios de Sidón Industrial reconoce el número 614-123-4567 como número pivote. Cualquier usuario que tenga registrado ese número pivote, sin importar su perfil, queda excluido de las notificaciones de Alertamiento por WhatsApp.
2. **Perfil Administrador.** Los usuarios con perfil Administrador no pueden recibir notificaciones de Alertamiento por WhatsApp. Si un usuario con perfil Administrador requiere recibir alertas, debe tener un segundo usuario dado de alta con el perfil correspondiente (Supervisor, Coordinador o Gerente).

### Preguntas frecuentes — Variables de control y Alertamiento por WhatsApp en Sidón Industrial

**¿Cuánto tarda en llegar la primera alerta de WhatsApp cuando una variable sale de rango?**
Sidón Industrial da un periodo de gracia de 15 minutos antes de disparar la primera alerta por WhatsApp, que se envía al perfil Supervisor.

**¿A quién escala la alerta si nadie resuelve la variable fuera de rango?**
El Alertamiento por WhatsApp escala primero a Supervisor, después de una hora escala también a Coordinador, y después de otra hora escala a Gerente. El esquema no tiene niveles después de Gerente.

**¿Hay que cerrar manualmente una alerta de WhatsApp en Sidón Industrial?**
No. Las alertas de Alertamiento por WhatsApp se resuelven solas cuando la variable regresa a su rango normal; no existe un botón de atender o cerrar.

**¿Por qué un Administrador no recibe alertas de WhatsApp?**
El perfil Administrador de Sidón Industrial está excluido por diseño del Alertamiento por WhatsApp. Necesita un segundo usuario con perfil Supervisor, Coordinador o Gerente para recibir notificaciones.

**¿Se detiene el Alertamiento por WhatsApp durante un paro programado?**
Sí, siempre que el paro programado esté dado de alta previamente en el catálogo de Paros programados de Sidón Industrial.

## 3.5 Histórico de turnos en Sidón Industrial

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *detalle de línea › pestaña Histórico de turnos*

La pantalla Histórico de turnos de Sidón Industrial es una tabla con el resultado de cada turno cerrado de la línea, con todos sus indicadores de OEE.

**Acciones en Histórico de turnos:**

- **Ver el detalle de un turno:** pulse el nombre del turno en Histórico de turnos. Se abre la vista Detalle de turno con la línea de tiempo de ese turno y la lista de sucesos.
- **Marcar un turno como no laborado:** úselo en Histórico de turnos cuando un turno no debía trabajarse. Sidón Industrial pide confirmación y excluye ese turno de los promedios.

> **Atención.** Marcar un turno como no laborado en Histórico de turnos cambia los indicadores acumulados de la línea. Confirme con su supervisor antes de marcar un turno como no laborado.

### Detalle de turno en Sidón Industrial

La vista Detalle de turno muestra el detalle del turno seleccionado y toda la información al momento de su cierre.

**Acciones en Detalle de turno:**

- **Turno anterior / turno siguiente:** permite navegar entre turnos dentro de Detalle de turno.
- **Ir al Turno Actual:** regresa al turno en curso en la pantalla Monitoreo de línea (ver sección 3.2 Monitoreo de línea).
- **Navegar por fecha:** permite navegar al primer turno de un día específico dentro de Detalle de turno.

### Preguntas frecuentes — Histórico de turnos en Sidón Industrial

**¿Qué pasa si marco un turno como no laborado en Histórico de turnos?**
Sidón Industrial excluye ese turno de los promedios y cambia los indicadores acumulados de la línea; por eso se recomienda confirmar con el supervisor antes de hacerlo.

**¿Cómo regreso al turno actual desde Detalle de turno?**
Pulse **Ir al Turno Actual** dentro de la vista Detalle de turno; Sidón Industrial regresa a la pantalla Monitoreo de línea con el turno en curso.

## 3.6 Resumen de planta en Sidón Industrial

**Perfil:** Supervisor, Coordinador, Gerente · **Ruta en Sidón Industrial:** *Dashboards › Resumen de planta*

La pantalla Resumen de planta de Sidón Industrial consolida el desempeño de la planta en un periodo. Resumen de planta descompone el OEE en cascada, de modo que se ve dónde se pierde el tiempo y el producto.

### Cómo se usa Resumen de planta

1. Elija sucursal, línea, turno y/o rango de fechas en Resumen de planta.
2. Pulse **Buscar** en Resumen de planta.
3. Cambie entre las pestañas **OEE Global** y **Producción** dentro de Resumen de planta.
4. En cada gráfica de Resumen de planta puede cambiar el tipo de gráfica (líneas, barras, área) y activar el apilado. Sidón Industrial recuerda esa elección.

### Preguntas frecuentes — Resumen de planta en Sidón Industrial

**¿Qué perfiles pueden ver Resumen de planta en Sidón Industrial?**
Resumen de planta está disponible para los perfiles Supervisor, Coordinador y Gerente.

**¿Qué pestañas tiene Resumen de planta?**
Resumen de planta tiene las pestañas OEE Global y Producción.

## 3.7 Tablero de desvíos de materia prima en Sidón Industrial

**Perfil:** todos los perfiles, excepto Operador · **Ruta en Sidón Industrial:** *Dashboards › Desviación / merma individual*

El Tablero de desvíos de materia prima analiza los desvíos registrados: por tipo, por turno, por estación, por día y por orden de producción.

> **Pantalla vacía.** Cuando no hay datos en el rango elegido, el Tablero de desvíos de materia prima de Sidón Industrial lo dice explícitamente con el aviso "No hay datos disponibles por el momento". Amplíe el rango de fechas o cambie los filtros del Tablero de desvíos de materia prima.

### Preguntas frecuentes — Tablero de desvíos de materia prima en Sidón Industrial

**¿Por qué el Tablero de desvíos de materia prima dice "No hay datos disponibles por el momento"?**
Ese aviso de Sidón Industrial aparece cuando no hay desvíos registrados en el rango de fechas o filtros elegidos. Amplíe el rango de fechas o cambie los filtros.

**¿El perfil Operador puede ver el Tablero de desvíos de materia prima?**
No. El Tablero de desvíos de materia prima está disponible para todos los perfiles de Sidón Industrial excepto Operador.
# Capítulo 4 · Ingreso de materia prima en Sidón Industrial

**Perfil:** Operador, Supervisor, Administrador · **Ruta en Sidón Industrial:** *Monitoreo de línea › Ingreso MP*

## 4.1 Cuándo se usa Ingreso de materia prima

Cada vez que se incorpora material a una orden de producción hay que registrarlo en el módulo Ingreso de materia prima de Sidón Industrial. Ese registro de Ingreso de materia prima alimenta el consumo real de la orden de producción, el cálculo de rendimiento y el análisis de costo. El proceso de Ingreso de materia prima tiene tres pasos desde la pantalla Monitoreo de línea: elegir la orden, elegir el modo de captura y registrar el material.

### Preguntas frecuentes — Cuándo se usa Ingreso de materia prima

**¿Para qué sirve el módulo Ingreso de materia prima en Sidón Industrial?**
El módulo Ingreso de materia prima registra el material incorporado a una orden de producción, y alimenta el consumo real, el rendimiento y el análisis de costo de esa orden.

## 4.2 Paso 1 · Elegir la orden en Ingreso de materia prima

Al pulsar el botón **Ingreso MP**, Sidón Industrial pregunta sobre qué orden de producción se va a trabajar en el módulo Ingreso de materia prima.

Opciones para elegir la orden en Ingreso de materia prima:

- **OP en Progreso** — la orden de producción que la línea está corriendo actualmente.
- **Otra OP** — para registrar material de otra orden de producción liberada.

Elija una opción y pulse **Continuar** en Ingreso de materia prima.

> **Sincronización automática.** Si la orden de producción aún no tiene sus materiales cargados en Sidón Industrial, el sistema los trae de SAP en ese momento. La sincronización con SAP puede tardar unos segundos.

### Preguntas frecuentes — Elegir la orden en Ingreso de materia prima

**¿Qué diferencia hay entre OP en Progreso y Otra OP en Ingreso de materia prima?**
OP en Progreso es la orden de producción que la línea está corriendo en ese momento; Otra OP permite elegir cualquier otra orden de producción liberada.

**¿Por qué tarda unos segundos al elegir una orden en Ingreso de materia prima?**
Si la orden de producción no tiene sus materiales cargados, Sidón Industrial los sincroniza automáticamente desde SAP, lo que puede tardar unos segundos.

## 4.3 Paso 2 · Elegir el modo de captura en Ingreso de materia prima

Modos de captura disponibles en el módulo Ingreso de materia prima:

| Modo de captura | Cuándo usarlo | Depende de |
|---|---|---|
| Captura por báscula | El peso se toma directamente del equipo. Permite descontar tara. En este modo es posible realizar varios ingresos en serie seleccionando el material de la tabla de la derecha. | Báscula conectada |
| Captura por código | Se escanea la etiqueta del material; el peso viene en el código. En este modo no es posible ingresar el código ni el peso manualmente: todo se toma de la etiqueta, la cual debe tener el layout de almacén establecido (35 y 37 caracteres). | Lector de códigos |
| Captura manual | Se teclean manualmente el código y el peso del material. | Nada |

> **Si la báscula no responde en Ingreso de materia prima.** Sidón Industrial reintenta la conexión con la báscula tres veces, mostrando el intento en curso. Si aun así falla, Sidón Industrial ofrece tres salidas: **Reintentar**, **Captura manual** o **Cancelar**.

### Preguntas frecuentes — Modos de captura en Ingreso de materia prima

**¿Qué modos de captura tiene Ingreso de materia prima?**
Ingreso de materia prima tiene tres modos de captura: Captura por báscula, Captura por código y Captura manual.

**La báscula no responde en Ingreso de materia prima, ¿qué hago?**
Sidón Industrial reintenta la conexión tres veces automáticamente. Si sigue sin conectar, use la opción Captura manual o Reintentar, o cancele la captura.

**¿Puedo escribir el código y el peso manualmente en Captura por código?**
No. En el modo Captura por código de Ingreso de materia prima, el código y el peso se toman directamente de la etiqueta escaneada; no se pueden capturar manualmente en ese modo.

## 4.4 Paso 3 · Registrar el material en Ingreso de materia prima

Al pasar el modo de captura, Sidón Industrial abre la pantalla de ingreso de Ingreso de materia prima. A la izquierda está el formulario de captura; a la derecha están las órdenes de producción con sus materiales y el avance de cada material.

Pasos para registrar el material en Ingreso de materia prima:

1. En el panel derecho de Ingreso de materia prima, localice la orden o suborden y pulse **Iniciar registro de MP**. Confirme cuando Sidón Industrial lo pida.
2. Sidón Industrial despliega la lista de materiales de esa orden de producción con su plan y su avance.
3. Escanee la etiqueta, teclee el código, o pulse el material en la lista, según el modo de captura seleccionado en Ingreso de materia prima.
4. Sidón Industrial valida el código y rellena el nombre del material y, si viene en la etiqueta, el peso.
5. Compruebe la cantidad registrada. En modo Captura por báscula, pulse **Obtener peso**.
6. El registro se agrega solo tras un segundo y aparece en la tabla inferior de Ingreso de materia prima.

### Descontar la tara en Ingreso de materia prima (solo en modo Captura por báscula)

1. Marque la casilla **Descontar tara** en Ingreso de materia prima.
2. Elija la tara del desplegable de taras.
3. Sidón Industrial resta el peso de la tara y deja el peso neto en la cantidad registrada.

> **Requisito.** Las taras deben estar dadas de alta en el catálogo de Taras (ver sección 6.5 Taras) para poder elegirlas en Ingreso de materia prima. Si el desplegable de taras está vacío, avise a su administrador de Sidón Industrial.

### Errores frecuentes al validar el código en modo escaneo de Ingreso de materia prima

| Mensaje de error | Qué significa y qué hacer |
|---|---|
| Código incompatible | La etiqueta no tiene el formato esperado por Sidón Industrial. Verifique que sea la etiqueta correcta o capture el material manualmente. |
| Esta etiqueta ya ha sido utilizada | Ese consecutivo de etiqueta ya se registró previamente en Sidón Industrial. No se puede duplicar el registro. |
| La materia prima no pertenece a la orden de producción actual | El material no está en el plan de esa orden de producción. Revise la orden o registre el material como material alterno. |
| Nivel incorrecto | El material pertenece a otra etapa o grafo del proceso. Termine la etapa en curso antes de pasar a la siguiente en Ingreso de materia prima. |
| El peso excede lo planeado | Requiere autorización por sobrepeso (ver sección 4.5 Autorización por sobrepeso). |

### Preguntas frecuentes — Registrar el material en Ingreso de materia prima

**¿Qué hago si el sistema dice "Código incompatible" al escanear en Ingreso de materia prima?**
Verifique que la etiqueta escaneada sea la correcta para ese material; si el problema persiste, use Captura manual en Ingreso de materia prima.

**¿Puedo descontar la tara con cualquier modo de captura?**
No. La opción Descontar tara de Ingreso de materia prima solo está disponible en el modo Captura por báscula.

**¿Qué hago si el desplegable de taras está vacío en Ingreso de materia prima?**
Avise a su administrador de Sidón Industrial: las taras deben estar dadas de alta previamente en el catálogo de Taras.

**¿Qué significa el mensaje "Nivel incorrecto" en Ingreso de materia prima?**
Significa que el material escaneado pertenece a otra etapa del proceso de producción. Debe terminar la etapa en curso antes de registrar ese material.

## 4.5 Autorización por sobrepeso en Ingreso de materia prima

Cuando la cantidad registrada en Ingreso de materia prima supera lo planeado para ese material, Sidón Industrial no la rechaza directamente: pide que alguien con rango autorice el sobrepeso.

Pasos de Autorización por sobrepeso en Ingreso de materia prima:

1. Sidón Industrial abre la lista de personas autorizadas para Autorización por sobrepeso. Busque por nombre, apellido o número de empleado.
2. Seleccione a la persona autorizante en la lista de Autorización por sobrepeso.
3. La persona autorizante escribe su correo y su contraseña para validarse en Autorización por sobrepeso.
4. Una vez autorizado el sobrepeso, la Autorización por sobrepeso queda vigente 30 minutos para el resto de la captura en Ingreso de materia prima.

> **Quién puede autorizar el sobrepeso.** Los perfiles Operador y auxiliar no aparecen en la lista de personas autorizadas de Sidón Industrial: nadie puede autorizarse a sí mismo un sobrepeso. Solamente los perfiles Supervisor, Coordinador y Gerente (o superiores) pueden generar la Autorización por sobrepeso.

### Preguntas frecuentes — Autorización por sobrepeso en Ingreso de materia prima

**¿Cuánto dura vigente una Autorización por sobrepeso en Sidón Industrial?**
La Autorización por sobrepeso queda vigente 30 minutos para el resto de la captura en Ingreso de materia prima.

**¿Puede un Operador autorizarse a sí mismo un sobrepeso?**
No. El perfil Operador no aparece en la lista de personas autorizadas de Sidón Industrial. Solo los perfiles Supervisor, Coordinador y Gerente pueden autorizar un sobrepeso.

## 4.6 Finalizar y salir en Ingreso de materia prima

En el módulo Ingreso de materia prima, los registros no se guardan uno a uno: quedan en la tabla inferior de Ingreso de materia prima hasta que el usuario pulsa **Finalizar**. Se puede eliminar cualquier renglón de la tabla antes de guardar.

Si el usuario intenta salir de Ingreso de materia prima con capturas pendientes, Sidón Industrial muestra cuántos registros y cuántos kilos tiene sin guardar, y ofrece tres opciones: **Guardar y salir**, **Salir sin guardar** o **Cancelar**.

> No pierda el trabajo capturado en Ingreso de materia prima: salvo que se haya equivocado, elija siempre Guardar y salir.

### Preguntas frecuentes — Finalizar y salir en Ingreso de materia prima

**¿Se guardan los registros de Ingreso de materia prima automáticamente?**
No. Los registros de Ingreso de materia prima quedan en la tabla inferior hasta que el usuario pulsa el botón Finalizar.

**¿Qué pasa si intento cerrar el navegador con capturas pendientes en Ingreso de materia prima?**
Sidón Industrial avisa cuántos registros y kilos hay sin guardar y ofrece Guardar y salir, Salir sin guardar o Cancelar. Si el navegador se cierra sin atender ese aviso, los ingresos que no se guardaron con Finalizar se pierden.

## 4.7 Consultar lo ingresado en Ingreso de materia prima

**Perfil:** todos los perfiles · **Ruta en Sidón Industrial:** *Costo y merma › Bitácora de ingresos de MP*, o el botón **Ver Bitácora de Ingreso de MP** de la propia pantalla de Ingreso de materia prima.

Al abrir una orden de producción en la Bitácora de ingresos de MP, se ven dos pestañas: **Plan contra real por material**, y la bitácora con cada ingreso registrado, su hora, cantidad y el modo de captura con que se registró. La Bitácora de ingresos de MP se actualiza sola cada minuto.

### Preguntas frecuentes — Consultar lo ingresado en Ingreso de materia prima

**¿Dónde consulto lo que ya se registró en Ingreso de materia prima?**
En la Bitácora de ingresos de MP, disponible en *Costo y merma › Bitácora de ingresos de MP*, o con el botón Ver Bitácora de Ingreso de MP desde la pantalla de Ingreso de materia prima.

**¿Cada cuánto se actualiza la Bitácora de ingresos de MP?**
La Bitácora de ingresos de MP se actualiza sola cada minuto en Sidón Industrial.
# Capítulo 5 · Registros de piso en Sidón Industrial

## 5.1 Desvío de materia prima en Sidón Industrial

**Perfil:** Operador, Supervisor · **Ruta en Sidón Industrial:** *Monitoreo de línea › Desvío MP*

El módulo Desvío de materia prima registra material que se sale del proceso: merma, producto rechazado o material desviado. Los registros de Desvío de materia prima afectan el indicador de Producto Conforme del OEE.

Pasos para registrar un Desvío de materia prima:

1. Pulse el botón **Desvío MP** desde la pantalla Monitoreo de línea. Sidón Industrial pide elegir una báscula para el Desvío de materia prima.
2. Sidón Industrial comprueba la conexión con el equipo y avisa del resultado de la conexión.
3. Sidón Industrial abre la ventana de captura de Desvío de materia prima con la lista de equipos de la línea.
4. Elija la orden de producción (viene precargada la orden en curso) y el tipo de desvío en Desvío de materia prima.
5. Marque los equipos afectados, dé clic en **peso**, seleccione la falla y, si procede, agregue un comentario en Desvío de materia prima.
6. Pulse **Guardar** en Desvío de materia prima.

> **Consejo.** Puede fijar con el ícono de chincheta los equipos que usa a diario en Desvío de materia prima: quedarán siempre arriba de la lista. Sidón Industrial recuerda esta preferencia por equipo.

> **Sobre el peso.** El botón de báscula de cada renglón en Desvío de materia prima toma la lectura del equipo. Si la báscula no está estable, Sidón Industrial lo advierte pero deja el valor capturado: verifique el peso antes de guardar el Desvío de materia prima.

> **Orden de captura.** Hasta que no elija el tipo de desvío, no podrá marcar equipos en Desvío de materia prima. Sidón Industrial avisa con el mensaje "Primero debe seleccionar un tipo de desvío".

### Preguntas frecuentes — Desvío de materia prima en Sidón Industrial

**¿Qué perfiles pueden registrar un Desvío de materia prima?**
Los perfiles Operador y Supervisor pueden registrar un Desvío de materia prima en Sidón Industrial.

**¿Qué indicador del OEE afecta el Desvío de materia prima?**
Los registros de Desvío de materia prima afectan el indicador de Producto Conforme del OEE.

**¿Por qué no puedo marcar equipos en Desvío de materia prima?**
Debe elegir primero el tipo de desvío; Sidón Industrial no permite marcar equipos hasta que el tipo de desvío esté seleccionado.

**¿Qué pasa si la báscula no está estable al registrar un Desvío de materia prima?**
Sidón Industrial advierte que la báscula no está estable pero deja el valor capturado; verifique el peso manualmente antes de pulsar Guardar.

## 5.2 Control de calidad en Sidón Industrial

**Perfil:** Operador, Supervisor, Coordinador, Administrador · **Ruta en Sidón Industrial:** *Monitoreo de línea › Control de Calidad*

El módulo Control de calidad levanta un control de calidad sobre la línea o sobre el producto, y registra ese control sobre la línea de tiempo de Sidón Industrial.

Pasos para levantar un Control de calidad:

1. Elija el tipo de aplicación desde la pantalla Monitoreo de línea: **Línea** o **Producto**, dentro de Control de calidad.
2. Elija el control a ejecutar en Control de calidad. La lista de controles se filtra según el tipo de aplicación elegido.
3. Indique el equipo de origen en Control de calidad.
4. Complete las actividades del control de calidad y pulse Guardar.

El Control de calidad queda marcado en la línea de tiempo del turno y se puede consultar después en la Bitácora de control de calidad (ver sección 8.4 Bitácora de control de calidad).

> **Requisito.** La lista de controles a elegir en Control de calidad viene del catálogo de Controles de calidad (ver sección 6.3 Controles de calidad). Si no hay ningún control dado de alta en ese catálogo, no aparecerá ninguna opción para seleccionar en Control de calidad. Si no aparecen opciones, contacte a su administrador de Sidón Industrial.

### Preguntas frecuentes — Control de calidad en Sidón Industrial

**¿Qué tipos de aplicación tiene Control de calidad?**
Control de calidad se puede aplicar sobre Línea o sobre Producto.

**No aparecen opciones de control en la pantalla Control de calidad, ¿por qué?**
La lista de controles depende del catálogo de Controles de calidad. Si ese catálogo está vacío, no aparecerá ninguna opción; contacte a su administrador de Sidón Industrial.

**¿Dónde puedo consultar los controles de calidad ya ejecutados?**
En la Bitácora de control de calidad de Sidón Industrial (sección 8.4).

## 5.3 Clasificar un paro en Sidón Industrial

**Perfil:** Operador, Supervisor, Gerente

Cuando la línea se detiene sin causa programada, Sidón Industrial abre un paro automáticamente y lo pinta en rojo en la línea de tiempo. Alguien debe indicar por qué ocurrió ese paro mediante el módulo Clasificar un paro.

> **Dos límites importantes.** No se puede clasificar un paro mientras la línea sigue detenida. Y si el paro fue hace más de siete días, ya no se puede clasificar en Sidón Industrial.

> **Cambiar de tipo deja rastro.** Pasar un paro de programado a no programado (o al revés) afecta la Disponibilidad del OEE. Sidón Industrial pide confirmación y registra quién hizo el cambio en la Bitácora de Paros (ver sección 8.3 Bitácora de paros).

Pasos para Clasificar un paro:

1. Desde la pantalla Monitoreo de línea o la vista Detalle de turno, pulse el ícono del paro.
2. Elija **Reclasificar** para asignarle motivo al paro, o **Dividir** si el paro tuvo dos causas distintas (ver sección 5.4 División de un paro).
3. Indique el tipo de paro, el tipo de motivo y el motivo concreto en Clasificar un paro.
4. Según el motivo elegido, Sidón Industrial pedirá también el equipo y el componente afectados.
5. Complete la descripción de la falla, la causa, la solución y los pendientes en Clasificar un paro.
6. Pulse **Guardar**. Según el tipo de paro seleccionado, el paro cambia de color en la línea de tiempo y se le inserta una etiqueta con el motivo declarado:
   - Paro No Programado: rojo intenso en la línea de tiempo.
   - Paro Programado: gris en la línea de tiempo.

### Preguntas frecuentes — Clasificar un paro en Sidón Industrial

**¿Por qué no puedo clasificar un paro que sigue activo?**
Sidón Industrial no permite clasificar un paro no programado mientras la línea sigue detenida. Debe esperar a que la línea arranque.

**¿Hasta cuándo puedo clasificar un paro después de que ocurrió?**
Un paro se puede clasificar hasta siete días después de haber ocurrido. Pasado ese límite, Sidón Industrial ya no permite clasificarlo.

**¿Qué pasa si cambio un paro de programado a no programado?**
Ese cambio afecta la Disponibilidad del OEE. Sidón Industrial pide confirmación y registra quién hizo el cambio en la Bitácora de Paros.

**¿Qué color toma un paro no programado ya clasificado en la línea de tiempo?**
Un paro no programado ya clasificado se muestra en rojo intenso en la línea de tiempo de Sidón Industrial.

## 5.4 División de un paro en Sidón Industrial

**Perfil:** Operador, Supervisor, Coordinador, Gerente

Cuando un mismo paro tuvo dos causas distintas que ocurrieron una después de otra dentro del mismo periodo de detención, y se necesita que cada tramo se clasifique y se contabilice por separado en el OEE, es posible usar el módulo División de un paro.

> **Dos límites importantes.** No se puede dividir un paro mientras no se haya cerrado en la línea de tiempo. Y si el paro fue hace más de siete días, ya no se puede dividir en Sidón Industrial.

Pasos para dividir un paro en Sidón Industrial:

1. Desde la pantalla Monitoreo de línea o la vista Detalle de turno, dé clic sobre el ícono del paro.
2. Elija **Dividir** para abrir el panel de División de un paro.
3. Arrastre el marcador (punto de corte) donde desea realizar la división. Las horas de inicio, fin y duraciones se ajustan automáticamente en la tabla inferior de División de un paro.
4. Pulse **Guardar**. El paro original se sustituye por dos registros independientes en la línea de tiempo.
5. Clasifique o reclasifique el primer tramo resultante: tipo de paro, motivo, equipo y componente si aplica (ver sección 5.3 Clasificar un paro).
6. Clasifique o reclasifique el segundo tramo resultante con la información correspondiente.

**Reglas de División de un paro en Sidón Industrial:**

- **Solo dos partes.** Un paro únicamente puede dividirse en dos tramos. Si se necesitan más de dos causas, hay que dividir por partes: se divide una vez y, si alguno de los dos tramos resultantes requiere subdividirse de nuevo, se repite el proceso de División de un paro sobre ese tramo.
- Ambas partes deben sumar exactamente la duración original del paro.
- **No se puede deshacer.** Una vez dividido un paro, no se puede volver a unir en un solo paro.
- **Límite de 7 días.** La división de un paro, igual que la clasificación de un paro, solo es posible dentro de los 7 días posteriores al origen del paro. Pasado ese lapso, el paro ya no se puede dividir ni clasificar en Sidón Industrial.

### Preguntas frecuentes — División de un paro en Sidón Industrial

**¿En cuántas partes se puede dividir un paro?**
Un paro solo se puede dividir en dos partes. Para más de dos causas, hay que repetir la División de un paro sobre uno de los tramos ya divididos.

**¿Se puede deshacer la división de un paro?**
No. Una vez dividido un paro en Sidón Industrial, no se puede volver a unir en un solo registro.

**¿Cuál es el límite de tiempo para dividir un paro?**
El límite es de 7 días después del origen del paro, el mismo límite que aplica para clasificar un paro.

**¿Las dos partes de un paro dividido deben sumar la duración original?**
Sí. Ambas partes resultantes de la División de un paro deben sumar exactamente la duración original del paro.
# Capítulo 6 · Catálogos en Sidón Industrial

**Perfil:** Administrador, Gerente · **Ruta en Sidón Industrial:** *Catálogos industriales*

Los catálogos de Sidón Industrial definen la información maestra con la que trabaja el resto del sistema. Todos los catálogos de Sidón Industrial funcionan igual: filtros arriba, tabla en el centro, y una ventana de alta o edición.

### Patrón común de los catálogos en Sidón Industrial

1. Escriba los filtros y pulse el botón de lupa para buscar en el catálogo.
2. El botón rojo con la cruz limpia los filtros del catálogo.
3. El botón **Nuevo +** abre la ventana de alta del catálogo.
4. Pulse cualquier renglón de la tabla del catálogo para editarlo.

### Preguntas frecuentes — Patrón común de los catálogos en Sidón Industrial

**¿Cómo doy de alta un registro nuevo en cualquier catálogo de Sidón Industrial?**
Pulse el botón Nuevo + dentro del catálogo correspondiente; Sidón Industrial abre la ventana de alta.

**¿Cómo limpio los filtros de un catálogo?**
Pulse el botón rojo con la cruz dentro del catálogo de Sidón Industrial; los filtros se limpian.

## 6.1 Catálogo de Productos en Sidón Industrial

El catálogo de Productos permite filtrar por familia de producto, por tipo (líquido o sólido) y por nombre de producto.

El catálogo de Productos es la base de la que se alimentan las órdenes de producción, el ingreso de materia prima y el cálculo de rendimiento por hora en Sidón Industrial. Si un producto no está dado de alta en el catálogo de Productos, o tiene mal cargado su peso por pieza, empaque o caja, los cálculos de producción y de OEE en las demás pantallas de Sidón Industrial saldrán incorrectos.

### Preguntas frecuentes — Catálogo de Productos en Sidón Industrial

**¿Por qué es importante cargar bien el catálogo de Productos?**
Porque el catálogo de Productos alimenta las órdenes de producción, el ingreso de materia prima y el cálculo de rendimiento por hora; un dato mal cargado (por ejemplo el peso por pieza) hace que los cálculos de producción y de OEE salgan incorrectos en todo Sidón Industrial.

**¿Por qué campos se puede filtrar el catálogo de Productos?**
El catálogo de Productos se puede filtrar por familia, por tipo (líquido o sólido) y por nombre.

## 6.2 Catálogo de Líneas de producción en Sidón Industrial

Al abrir una línea en el catálogo de Líneas de producción se configuran sus equipos y el papel de cada equipo: si es equipo clave, si es báscula, si es detector de metales, si es báscula dinámica y si cuenta para la Disponibilidad del OEE.

> **Cambios sensibles.** Marcar o desmarcar la opción "cuenta para disponibilidad OEE" en el catálogo de Líneas de producción cambia cómo se calcula el indicador de la línea. Hágalo solo con autorización.

### Preguntas frecuentes — Catálogo de Líneas de producción en Sidón Industrial

**¿Qué se configura al abrir una línea en el catálogo de Líneas de producción?**
Se configuran los equipos de la línea y el papel de cada equipo: si es equipo clave, báscula, detector de metales, báscula dinámica, y si cuenta para la Disponibilidad del OEE.

**¿Qué pasa si desmarco "cuenta para disponibilidad OEE" en una línea?**
Cambia cómo se calcula el indicador de Disponibilidad de esa línea; esta acción debe hacerse solo con autorización.

## 6.3 Catálogo de Controles de calidad en Sidón Industrial

El catálogo de Controles de calidad contiene las plantillas de control de calidad, clasificadas por aplicación (producto o línea). Cada plantilla del catálogo de Controles de calidad define las actividades que el operador deberá completar al ejecutar el control en la pantalla Control de calidad (ver sección 5.2 Control de calidad).

### Preguntas frecuentes — Catálogo de Controles de calidad en Sidón Industrial

**¿Para qué sirve el catálogo de Controles de calidad?**
El catálogo de Controles de calidad define las plantillas y actividades que un operador debe completar al ejecutar un control de calidad en la pantalla Control de calidad.

**¿Qué pasa si el catálogo de Controles de calidad está vacío?**
Si el catálogo de Controles de calidad no tiene plantillas dadas de alta, no aparecerá ninguna opción para elegir en la pantalla Control de calidad.

## 6.4 Catálogo de Materias primas en Sidón Industrial

**Origen de los datos.** El catálogo de Materias primas se autoalimenta con la consulta de materiales desde SAP, aunque también es posible dar de alta materias primas manualmente cuando haga falta.

**Catálogo sensible.** Modificar un registro del catálogo de Materias primas puede repercutir en los módulos de Ingreso de materia prima y Cálculo de costos, ya que ambos módulos toman la información directamente del catálogo de Materias primas. Modifique el catálogo de Materias primas con precaución y solo con la autorización correspondiente.

### Preguntas frecuentes — Catálogo de Materias primas en Sidón Industrial

**¿De dónde vienen los datos del catálogo de Materias primas?**
El catálogo de Materias primas se autoalimenta desde SAP mediante consulta de materiales, aunque también permite alta manual cuando haga falta.

**¿Qué módulos se ven afectados si modifico el catálogo de Materias primas?**
Modificar el catálogo de Materias primas afecta a Ingreso de materia prima y a Cálculo de costos, porque ambos módulos toman su información directamente de ese catálogo. Debe modificarse con precaución y con la autorización correspondiente.

## 6.5 Catálogo de Taras en Sidón Industrial

Las taras que se den de alta en el catálogo de Taras son las que el operador podrá descontar al pesar en el módulo Ingreso de materia prima (ver sección 4.4 Descontar la tara).

Al dar de alta una tara en el catálogo de Taras se piden tres campos: descripción, peso y unidad de medida. Los tres campos del catálogo de Taras son obligatorios y el peso debe ser mayor que cero. Las taras del catálogo de Taras se pueden eliminar; Sidón Industrial pide confirmación antes de eliminar una tara.

### Preguntas frecuentes — Catálogo de Taras en Sidón Industrial

**¿Qué campos son obligatorios al dar de alta una tara?**
Los tres campos del catálogo de Taras son obligatorios: descripción, peso y unidad de medida; el peso debe ser mayor que cero.

**¿Para qué sirve el catálogo de Taras?**
Las taras dadas de alta en el catálogo de Taras son las que el operador puede descontar al capturar el peso en el módulo Ingreso de materia prima, en modo Captura por báscula.

---

# Capítulo 7 · Paros programados en Sidón Industrial

**Perfil:** Coordinador, Supervisor, Gerente · **Ruta en Sidón Industrial:** *Catálogos industriales › Paros programados*

El catálogo de Paros programados permite registrar por adelantado los paros previstos: limpieza, mantenimiento, cambio de formato. Un paro dado de alta en el catálogo de Paros programados no se cuenta como paro no programado y no penaliza la Disponibilidad del OEE; además, un paro programado permite silenciar el Alertamiento por WhatsApp durante su periodo designado (ver sección 3.4 Alertamiento por WhatsApp).

### Buscar en el catálogo de Paros programados

El catálogo de Paros programados tiene dos rangos de fecha independientes: el rango de inicio del paro y el rango de creación del registro. También se puede filtrar por línea y por quién creó el registro.

> **Se requiere al menos un filtro.** Si se pulsa Buscar en el catálogo de Paros programados con todos los filtros vacíos, Sidón Industrial avisa: "Debes ingresar al menos un filtro para realizar la búsqueda".

### Programar un paro en Sidón Industrial

Pasos para programar un paro en el catálogo de Paros programados:

1. Pulse **Nuevo** en el catálogo de Paros programados.
2. Elija la línea, el tipo de motivo y el motivo del paro programado.
3. Indique si el paro se repite: **No** (paro único; indique fecha de inicio y fin) o **Sí** (paro recurrente; marque los días de la semana L M M J V S D).
4. Indique la hora y la duración del paro programado.
5. Pulse **Guardar**.

### Preguntas frecuentes — Paros programados en Sidón Industrial

**¿Un paro programado afecta la Disponibilidad del OEE?**
No. Un paro dado de alta en el catálogo de Paros programados de Sidón Industrial no se cuenta como paro no programado y no penaliza la Disponibilidad del OEE.

**¿Puedo programar un paro que se repita varios días a la semana?**
Sí. Al programar un paro puede indicar que se repite (opción Sí) y marcar los días de la semana en los que aplica.

**¿Qué pasa si busco en el catálogo de Paros programados sin ningún filtro?**
Sidón Industrial avisa que se requiere al menos un filtro para realizar la búsqueda; no permite buscar con todos los campos vacíos.

**¿Cómo se relaciona un paro programado con el Alertamiento por WhatsApp?**
Mientras la línea está en un paro dado de alta en el catálogo de Paros programados, el Alertamiento por WhatsApp se suspende durante ese periodo.
# Capítulo 8 · Consultas y bitácoras en Sidón Industrial

**Perfil:** todos los perfiles, excepto Operador

Las consultas y bitácoras de Sidón Industrial permiten revisar lo ocurrido y exportarlo a Excel. Todas las consultas y bitácoras comparten el mismo patrón: filtros arriba, botones de buscar, limpiar y exportar, y tabla paginada con un buscador libre.

> **Rango por defecto.** Casi todas las consultas de Sidón Industrial se abren mostrando del día 1 del mes en curso al día actual. Ajuste las fechas si necesita otro periodo.

### Preguntas frecuentes — Consultas y bitácoras en Sidón Industrial

**¿Qué rango de fechas muestran las consultas por defecto?**
Casi todas las consultas de Sidón Industrial se abren mostrando del día 1 del mes en curso hasta el día actual.

**¿El perfil Operador puede usar las consultas y bitácoras?**
No. Las consultas y bitácoras de Sidón Industrial están disponibles para todos los perfiles excepto Operador.

## 8.1 Consulta de Órdenes de producción en Sidón Industrial

**Ruta en Sidón Industrial:** *Costo y merma › Bitácora de ingresos MP*

La consulta de Órdenes de producción muestra cantidad a producir, producido, diferencia, fechas, porcentaje de avance y estatus de cada orden. El estatus de una orden en esta consulta puede ser: en ejecución o detenida.

### Preguntas frecuentes — Consulta de Órdenes de producción en Sidón Industrial

**¿Qué estatus puede tener una orden en la consulta de Órdenes de producción?**
Una orden de producción puede tener estatus en ejecución o detenida en esta consulta de Sidón Industrial.

## 8.2 Histórico de variables en Sidón Industrial

Pasos para usar la consulta Histórico de variables:

1. Elija sucursal, línea, equipo y variable, en ese orden, en la consulta Histórico de variables: cada selección filtra la siguiente.
2. Ajuste el rango de fechas en Histórico de variables. Por defecto Sidón Industrial muestra los últimos cuatro días.
3. Pulse **Buscar** en Histórico de variables.

En la gráfica de Histórico de variables es posible hacer zoom, ya sea con pantalla touch o con el scroll del mouse. Estando en una sección con zoom, es posible moverse de forma lateral dentro de la gráfica.

> **Máximo tres meses.** Si se elige un rango mayor a tres meses en Histórico de variables, Sidón Industrial recorta el rango y avisa del nuevo rango aplicado.

> **Consejo.** Las consultas amplias en Histórico de variables tardan más. Sidón Industrial avisa con "Esto puede tardar unos segundos". Para ver el detalle minuto a minuto, use rangos de cuatro días o menos en Histórico de variables.

### Preguntas frecuentes — Histórico de variables en Sidón Industrial

**¿Cuál es el rango de fechas por defecto en Histórico de variables?**
El rango por defecto de Histórico de variables son los últimos cuatro días.

**¿Cuál es el rango máximo permitido en Histórico de variables?**
El rango máximo es de tres meses. Si se elige un rango mayor, Sidón Industrial lo recorta automáticamente y avisa.

**¿Cómo se llega al Histórico de variables desde Variables de control?**
Desde cada gráfica de la pantalla Variables de control (sección 3.4), se puede dar clic sobre el nombre de la variable para abrir el Histórico de variables ya filtrado.

## 8.3 Bitácora de paros en Sidón Industrial

La Bitácora de paros muestra fechas, equipo, componente, motivo, descripción de la falla, causa, solución, pendientes y estatus de mantenimiento de cada paro.

**Acciones en la Bitácora de paros:**

- Pulse un renglón de la Bitácora de paros para ver todo el historial de ese paro: cada intervención y reclasificación queda registrada con su fecha y su responsable.
- Desde la propia Bitácora de paros se puede reclasificar un paro.
- El botón **Exportar** genera el Excel con lo que esté viendo en la Bitácora de paros.

### Preguntas frecuentes — Bitácora de paros en Sidón Industrial

**¿Puedo reclasificar un paro desde la Bitácora de paros?**
Sí. Desde la propia Bitácora de paros se puede reclasificar un paro, sujeto a los mismos límites de la sección 5.3 Clasificar un paro (por ejemplo, el límite de 7 días).

**¿Qué información muestra el historial de un paro en la Bitácora de paros?**
Al pulsar un renglón de la Bitácora de paros se ve todo el historial de ese paro, incluyendo cada intervención y reclasificación con su fecha y responsable.

## 8.4 Bitácora de control de calidad en Sidón Industrial

Al pulsar un renglón de la Bitácora de control de calidad se ve el detalle completo del control ejecutado, con todas sus actividades y resultados. Para maximizar las imágenes del detalle, dé clic sobre el ícono de "ojo" en la Bitácora de control de calidad.

### Preguntas frecuentes — Bitácora de control de calidad en Sidón Industrial

**¿Cómo veo el detalle de un control de calidad ejecutado?**
Pulse el renglón correspondiente en la Bitácora de control de calidad; Sidón Industrial muestra el detalle completo con actividades y resultados.

**¿Cómo maximizo las imágenes de un control en la Bitácora de control de calidad?**
Dé clic sobre el ícono de "ojo" dentro del detalle de la Bitácora de control de calidad.

## 8.5 Bitácora del detector de metales en Sidón Industrial

La Bitácora del detector de metales muestra las alertas del detector de metales, con el tiempo de atención y el cierre de cada alerta.

### Preguntas frecuentes — Bitácora del detector de metales en Sidón Industrial

**¿Qué información muestra la Bitácora del detector de metales?**
La Bitácora del detector de metales muestra las alertas del detector de metales, con el tiempo de atención y el cierre de cada alerta.

## 8.6 Etiquetas de producción en Sidón Industrial

Pasos para descargar Etiquetas de producción:

1. Elija la línea y la fecha en la pantalla Etiquetas de producción.
2. Pulse el botón de descarga. Se obtiene un archivo Excel llamado `Etiquetas_{línea}_{fecha}.xlsx`. En este Excel se pueden visualizar las etiquetas recibidas desde piso de producción.

Sidón Industrial toma el día en Etiquetas de producción como un "día-turnos", donde el día inicia y finaliza a las 7:00 am, con el inicio del primer turno y el fin del tercer turno del día anterior. En el exportable de Etiquetas de producción se puede consultar la fecha y hora exacta (columna D) en la que se recibió cada etiqueta en el sistema.

> **Hay un límite de descargas.** Cada línea tiene una cuota de descargas en Etiquetas de producción. Al agotarla, Sidón Industrial avisa "Se ha alcanzado el límite de descargas permitidas para esta línea". Consulte con su administrador de Sidón Industrial.

### Preguntas frecuentes — Etiquetas de producción en Sidón Industrial

**¿Qué formato tiene el archivo descargado de Etiquetas de producción?**
El archivo descargado es un Excel llamado Etiquetas_{línea}_{fecha}.xlsx.

**¿Qué pasa si se agota la cuota de descargas de Etiquetas de producción?**
Sidón Industrial avisa que se alcanzó el límite de descargas permitidas para esa línea; hay que consultar con el administrador de Sidón Industrial.

**¿Cómo define Sidón Industrial el "día" en Etiquetas de producción?**
Como un día-turnos que inicia y termina a las 7:00 am, coincidiendo con el inicio del primer turno y el fin del tercer turno del día anterior.

---

# Capítulo 9 · Costo y merma en Sidón Industrial

**Perfil:** Administrador, Coordinador, Gerente · **Ruta en Sidón Industrial:** *Costo y merma*

## 9.1 Bitácora de desvíos en Sidón Industrial

La Bitácora de desvíos muestra fecha, turno, usuario, línea, equipo, orden, producto, tipo de desvío, falla, peso y acumulados del turno y de la orden.

En la Bitácora de desvíos se puede filtrar por sucursal, línea, turno, equipo y fechas. La columna de acumulados de la Bitácora de desvíos permite ver cuánto se lleva desviado en el turno y en la orden. La Bitácora de desvíos es exportable a Excel.

### Preguntas frecuentes — Bitácora de desvíos en Sidón Industrial

**¿Por qué campos se puede filtrar la Bitácora de desvíos?**
La Bitácora de desvíos se puede filtrar por sucursal, línea, turno, equipo y fechas.

**¿Qué muestra la columna de acumulados en la Bitácora de desvíos?**
Muestra cuánto material se lleva desviado en el turno y en la orden de producción.

## 9.2 Resumen de impacto en costos en Sidón Industrial

La pantalla Resumen de impacto en costos tiene cuatro vistas:

| Pestaña de Resumen de impacto en costos | Qué muestra |
|---|---|
| Resumen | Agregado por familia: cantidad real, importes real, plan y estándar, impacto y cumplimiento del plan. Incluye fila de totales. |
| Elementos | Detalle por producto: volumen, rendimientos, gasto de fabricación, seco, empaque, merma, cárnico alterno y descongele. |
| Órdenes | Detalle completo orden por orden: volúmenes planeados y reales, cumplimiento, costos, variaciones, rendimientos, fechas y días de retraso. |
| Desglose | Detalle desglosado de la orden de producción, acomodado respectivamente en cada uno de los grafos de esa orden. |

> **Consejo.** La vista de Órdenes dentro de Resumen de impacto en costos es muy ancha. Desplácese horizontalmente dentro de la tabla o exporte a Excel para trabajarla con comodidad.

### Preguntas frecuentes — Resumen de impacto en costos en Sidón Industrial

**¿Cuántas vistas tiene Resumen de impacto en costos?**
Resumen de impacto en costos tiene cuatro vistas: Resumen, Elementos, Órdenes y Desglose.

**¿Qué muestra la pestaña Órdenes de Resumen de impacto en costos?**
Muestra el detalle completo orden por orden: volúmenes planeados y reales, cumplimiento, costos, variaciones, rendimientos, fechas y días de retraso.

**La vista de Órdenes es muy ancha, ¿cómo la reviso más fácil?**
Desplácese horizontalmente dentro de la tabla, o exporte la vista de Órdenes a Excel para trabajarla con más comodidad.
---

# Anexo A · Glosario de Sidón Industrial

| Término | Significado en Sidón Industrial |
|---|---|
| OEE | Eficiencia general del equipo. Resume en un solo número el desempeño de la línea. El OEE resulta de multiplicar Disponibilidad, Desempeño y Producto conforme. |
| Disponibilidad | Porcentaje del tiempo disponible en que la línea realmente pudo producir, descontando los paros no programados. |
| Desempeño | Cuánto se produjo frente a lo que se esperaba producir en ese tiempo. |
| Producto conforme | Porcentaje de lo producido que salió bien, descontando los desvíos de materia prima. |
| OP | Orden de producción. |
| Turno | Periodo de trabajo definido para la sucursal. Los indicadores del OEE se acumulan por turno. |
| Paro programado | Detención prevista y registrada por adelantado en Sidón Industrial. No penaliza la Disponibilidad. |
| Paro no programado | Detención imprevista. Penaliza la Disponibilidad y debe clasificarse en Sidón Industrial. |
| Desvío de MP | Material que sale del proceso: merma, rechazo o desviación. |
| Tara | Peso del recipiente, que se descuenta para obtener el peso neto del material en Sidón Industrial. |
| Material alterno | Material que se usa sin estar en el plan original de la orden de producción. |
| UMB | Unidad de medida base del producto: pieza, caja, paquete. |

### Preguntas frecuentes — Glosario de Sidón Industrial

**¿Qué significa OEE en Sidón Industrial?**
OEE es la Eficiencia General del Equipo, resultado de multiplicar Disponibilidad, Desempeño y Producto conforme.

**¿Qué diferencia hay entre paro programado y paro no programado?**
Un paro programado se registra por adelantado en Sidón Industrial y no penaliza la Disponibilidad; un paro no programado es una detención imprevista que sí penaliza la Disponibilidad y debe clasificarse.

---

# Anexo B · Colores y códigos de Sidón Industrial

## Estado de la línea en Sidón Industrial

| Indicador de estado de línea | Significado |
|---|---|
| Disponible | La línea está produciendo. |
| Inactiva | La línea no está operando y no hay paro registrado. |
| En paro | La línea está detenida. |

## Colores de la línea de tiempo en Sidón Industrial

| Color en la línea de tiempo | Significado |
|---|---|
| Verde | Producción normal. |
| Amarillo | Producción baja (80% o menos de lo esperado). |
| Rojo | Paro no programado sin clasificar. |
| Rojo intenso | Paro no programado ya clasificado. |
| Gris | Paro programado. |
| Blanco | Sin información para ese minuto. |

## Íconos de suceso en la línea de tiempo de Sidón Industrial

| Suceso en la línea de tiempo | Al pulsarlo |
|---|---|
| Paro programado | Abre el detalle del paro. |
| Paro no programado | Permite clasificarlo o dividirlo. |
| Control de calidad | Muestra el control ejecutado. |
| Cambio de orden | Solo informativo. |
| Detección de metal | Solo informativo. |

### Preguntas frecuentes — Colores y códigos de Sidón Industrial

**¿Qué significa el color amarillo en la línea de tiempo de Sidón Industrial?**
El color amarillo significa que la línea está produciendo por debajo del 80% de lo esperado.

**¿Qué pasa si pulso el ícono de un paro no programado en la línea de tiempo?**
Sidón Industrial permite clasificarlo o dividirlo desde ese ícono.

**¿Qué diferencia hay entre rojo y rojo intenso en la línea de tiempo?**
El rojo indica un paro no programado sin clasificar; el rojo intenso indica un paro no programado que ya fue clasificado.

---

# Anexo C · Preguntas frecuentes generales de Sidón Industrial

**No veo una opción del menú que aparece en este manual, ¿qué hago?**
Su perfil de Sidón Industrial no la tiene autorizada. Solicítela a su administrador indicando la sección y la pantalla.

**La pantalla de Sidón Industrial dice "No hay datos disponibles por el momento", ¿qué significa?**
No hay registros en el periodo o con los filtros elegidos. Amplíe el rango de fechas o limpie los filtros con el botón rojo.

**Los indicadores de Sidón Industrial no cambian, ¿qué hago?**
El Monitoreo de línea se actualiza solo cada pocos segundos y el Resumen de líneas cada diez minutos. Si aun así no cambian, verifique que no esté viendo un turno anterior: pulse Ir al Turno Actual.

**La báscula no responde en Sidón Industrial, ¿qué hago?**
Sidón Industrial reintenta la conexión tres veces. Si sigue sin conectar, compruebe que el equipo esté encendido y en red; mientras tanto, continúe con Captura manual.

**Registré un ingreso equivocado en Ingreso de materia prima, ¿cómo lo corrijo?**
Si aún no ha pulsado Finalizar, elimine el renglón de la tabla inferior de Ingreso de materia prima. Si ya lo guardó, avise a su supervisor: la corrección no se hace desde esa pantalla.

**No puedo clasificar un paro en Sidón Industrial, ¿por qué?**
Si la línea sigue detenida, espere a que arranque. Si el paro tiene más de siete días, ya no se puede cambiar su tipo.

**Sidón Industrial me pide autorización por sobrepeso, ¿qué significa?**
La cantidad capturada supera lo planeado para ese material. Se necesita que un Supervisor o Coordinador se identifique con su usuario y contraseña. La Autorización por sobrepeso vale 30 minutos.

**Cerré el navegador sin guardar en Sidón Industrial, ¿perdí mi captura?**
Los ingresos que no se guardaron con el botón Finalizar se pierden. Sidón Industrial avisa antes de salir; no ignore ese aviso.
