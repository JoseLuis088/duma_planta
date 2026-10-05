/* ============================================================================
   Las cinco pantallas que faltaban por nombrar.

   EL SINTOMA. En el tablero seguian saliendo renglones asi:

     GET /api/productionOrders/sapExist      4
     GET /api/rawMaterialEntry               2
     POST /api/productionOrders/productionData   1
     PUT /api/users/password                 1

   No es un fallo: es el comportamiento previsto. Una ruta que no esta en
   dbo.pantallas se ensena cruda, en gris, porque es feo pero honesto -mejor eso
   que inventarle un nombre-. Lo que toca es nombrarlas.

   DOS DE ELLAS COMPARTEN NOMBRE A PROPOSITO. El tablero agrupa por `nombre`, no
   por `ruta`, asi que `rawMaterialEntry` y la consulta de la bascula caen en un
   solo renglon de Ingreso de materia prima. Para un director eso es una pantalla,
   no dos; que por debajo sean dos llamadas distintas es cosa nuestra. Lo mismo
   con sapExist, que es un paso dentro de Ordenes de produccion y no una pantalla
   aparte en el menu de nadie.

   SIGUEN TODAS EN confirmado = 0. Los nombres salen del manual de Sidon -el
   capitulo 4 es Ingreso de materia prima, y el cambio de contrasena aparece en
   el capitulo de acceso-, pero QUE ESE ENDPOINT SIRVA A ESA PANTALLA sigue
   siendo deduccion nuestra. La bandera no dice si el nombre suena bien, dice si
   alguien de Sidon ya lo valido. Nadie lo ha hecho todavia: van con las otras 26
   cuando Joel las revise.
   ============================================================================ */

USE Sidon_Uso;
GO

MERGE dbo.pantallas AS d
USING (VALUES
    -- Produccion
    (N'GET /api/productionOrders/sapExist',
     N'Órdenes de producción',    N'Producción', N'📦', 0,
     N'Comprueba si la orden ya existe en SAP; es un paso dentro de Órdenes de producción.'),

    (N'POST /api/productionOrders/productionData',
     N'Captura de producción',    N'Producción', N'✍️', 0,
     N'Registra los datos de producción de una orden. Deducido del nombre del endpoint.'),

    -- Materia prima: las dos caen en la misma pantalla del menu
    (N'GET /api/rawMaterialEntry',
     N'Ingreso de materia prima', N'Calidad',    N'🧺', 0,
     N'Capítulo 4 del manual. Misma pantalla que GET /api/rawMaterial.'),

    (N'GET /api/DeviceIndustrialDetail/scaleByProductionLine/{id}',
     N'Ingreso de materia prima', N'Calidad',    N'🧺', 0,
     N'La báscula de la línea: es el modo Captura por báscula de esa misma pantalla.'),

    -- Sistema
    (N'PUT /api/users/password',
     N'Cambio de contraseña',     N'Sistema',    N'🔒', 0,
     N'Sidón lo pide en el primer inicio de sesión (capítulo de acceso del manual).')
) AS o (ruta, nombre, area, icono, confirmado, nota)
ON d.ruta = o.ruta
WHEN NOT MATCHED THEN
    INSERT (ruta, nombre, area, icono, confirmado, nota)
    VALUES (o.ruta, o.nombre, o.area, o.icono, o.confirmado, o.nota);
GO

/* Que no quede ninguna sin nombre de las que la gente usa de verdad. Si esta
   consulta devuelve filas, son pantallas nuevas de Sidon que hay que nombrar:
   pasa cada vez que ellos publican un endpoint, asi que conviene correrla de
   tanto en tanto. */
SELECT u.ruta, SUM(u.peticiones) AS peticiones
FROM dbo.uso_diario u
JOIN dbo.cuentas c ON c.usuario = u.usuario AND c.es_persona = 1
LEFT JOIN dbo.pantallas p ON p.ruta = u.ruta
WHERE p.ruta IS NULL
GROUP BY u.ruta
ORDER BY peticiones DESC;
GO
