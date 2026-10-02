/* ============================================================================
   Nombres de pantalla para los directivos.

   EL PROBLEMA. El tablero ensenaba esto:

     GET /api/productionLines/{id}/fullData      26
     GET /api/workShiftHistoric                   3
     POST /api/users/login                        1

   Un director no tiene por que saber que la primera es la pantalla de monitoreo.
   Y `Module`, que seria el candidato natural, no sirve: la mitad de sus valores
   son GUIDs.

   POR QUE UNA TABLA Y NO UN DICCIONARIO EN EL CODIGO. Varios de los nombres de
   abajo son DEDUCIDOS del nombre del endpoint, no confirmados con nadie de Sidon.
   Algunos son evidentes (`workShiftHistoric` es el Historico de turnos) y otros
   son una apuesta razonable. Puestos en una tabla, quien sepa la respuesta los
   corrige con un UPDATE; escondidos en el codigo, habria que desplegar.

   Por eso cada fila lleva `confirmado`: 0 mientras sea deduccion nuestra, 1 en
   cuanto alguien de Sidon la valide. El tablero puede marcar las dudosas.

   LAS RUTAS QUE NO ESTEN AQUI no se pierden: el tablero ensena la ruta cruda, que
   es feo pero honesto, y asi se ve cual falta por nombrar.

   Los nombres salen del manual de usuario de Sidon Industrial, para que coincidan
   con lo que la gente ve en su menu.
   ============================================================================ */

USE Sidon_Uso;
GO

IF OBJECT_ID(N'dbo.pantallas') IS NULL
CREATE TABLE dbo.pantallas (
    ruta        NVARCHAR(200) NOT NULL PRIMARY KEY,
    nombre      NVARCHAR(120) NOT NULL,
    area        NVARCHAR(60)  NOT NULL,
    icono       NVARCHAR(12)  NOT NULL,
    confirmado  BIT           NOT NULL DEFAULT 0,
    nota        NVARCHAR(200) NULL
);
GO

MERGE dbo.pantallas AS d
USING (VALUES
    -- Tableros de seguimiento (Capitulo 3 del manual)
    (N'GET /api/productionLines/{id}/fullData',      N'Monitoreo de línea',        N'Seguimiento', N'📊', 0),
    (N'GET /api/productionLines/basic/{id}',         N'Resumen de líneas',         N'Seguimiento', N'🏭', 0),
    (N'GET /api/productionLines',                    N'Resumen de líneas',         N'Seguimiento', N'🏭', 0),
    (N'GET /api/productionLines/lastWorkShift',      N'Último turno',              N'Seguimiento', N'🕐', 0),
    (N'GET /api/productionLines/inExecution/{id}',   N'Orden en ejecución',        N'Seguimiento', N'▶️', 0),
    (N'GET /api/workShiftHistoric',                  N'Histórico de turnos',       N'Seguimiento', N'📅', 0),
    (N'GET /api/workShiftExecutions',                N'Histórico de turnos',       N'Seguimiento', N'📅', 0),

    -- Variables de control y sensores
    (N'GET /api/controlVariables',                   N'Variables de control',      N'Seguimiento', N'🌡️', 0),
    (N'GET /api/devices',                            N'Equipos',                   N'Seguimiento', N'⚙️', 0),
    (N'GET /api/deviceComponents',                   N'Componentes de equipo',     N'Seguimiento', N'⚙️', 0),

    -- Paros (Capitulo 5 y 8)
    (N'GET /api/motives',                            N'Motivos de paro',           N'Paros',       N'⏸️', 0),
    (N'GET /api/motiveTypes',                        N'Tipos de paro',             N'Paros',       N'⏸️', 0),
    (N'GET /api/stopages',                           N'Bitácora de paros',         N'Paros',       N'📋', 0),
    (N'GET /api/scheduledStopages',                  N'Paros programados',         N'Paros',       N'🗓️', 0),

    -- Produccion
    (N'POST /api/sys/upsertProductionOrder',         N'Órdenes de producción',     N'Producción',  N'📦', 0),
    (N'GET /api/productionOrders',                   N'Órdenes de producción',     N'Producción',  N'📦', 0),

    -- Calidad y materia prima
    (N'GET /api/qualityControls',                    N'Control de calidad',        N'Calidad',     N'✅', 0),
    (N'GET /api/rawMaterial',                        N'Ingreso de materia prima',  N'Calidad',     N'🧺', 0),
    (N'GET /api/deviations',                         N'Desvíos de materia prima',  N'Calidad',     N'⚠️', 0),

    -- Costo y merma (Capitulo 9)
    (N'GET /api/CostAndScrap/GetAllSummarizedOrders', N'Costo y merma',            N'Costos',      N'💰', 0),

    -- Alertas
    (N'GET /api/alerts/health',                      N'Estado de alertas',         N'Alertas',     N'🔔', 0),
    (N'GET /api/alerts',                             N'Alertas',                   N'Alertas',     N'🔔', 0),
    (N'DELETE /api/sys/alerts/{id}',                 N'Alertas',                   N'Alertas',     N'🔔', 0),

    -- Catalogos y sistema
    (N'GET /api/locations',                          N'Catálogo de ubicaciones',   N'Catálogos',   N'📍', 0),
    (N'GET /api/products',                           N'Catálogo de productos',     N'Catálogos',   N'🏷️', 0),
    (N'GET /api/users',                              N'Usuarios',                  N'Sistema',     N'👤', 0),
    (N'POST /api/users/login',                       N'Entrada al sistema',        N'Sistema',     N'🔑', 1)
) AS o (ruta, nombre, area, icono, confirmado)
ON d.ruta = o.ruta
WHEN NOT MATCHED THEN
    INSERT (ruta, nombre, area, icono, confirmado)
    VALUES (o.ruta, o.nombre, o.area, o.icono, o.confirmado);
GO

PRINT 'Tabla de pantallas lista. Las marcadas confirmado=0 son deduccion nuestra.';
GO

/* Para ver que rutas usa la gente y todavia no tienen nombre:

   SELECT u.ruta, SUM(u.peticiones) AS peticiones
   FROM dbo.uso_diario u
   JOIN dbo.cuentas c ON c.usuario = u.usuario AND c.es_persona = 1
   LEFT JOIN dbo.pantallas p ON p.ruta = u.ruta
   WHERE p.ruta IS NULL
   GROUP BY u.ruta ORDER BY peticiones DESC;
*/
