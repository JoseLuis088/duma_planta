/* ============================================================================
   Dos columnas que la primera corrida con datos reales demostro que faltaban.

   1. logins.usuario_id

   De 19 logins del 30 de septiembre, 17 salieron sin correo. Tiene sentido: en
   el momento del POST de login el sistema todavia no sabe quien eres -se lo
   estas preguntando- asi que escribe la fila sin correo. Pero SI escribe el
   UserId, y ese mismo UserId aparece con correo en las miles de peticiones ya
   autenticadas de esa persona. El ETL arma el mapa en la misma pasada y les pone
   nombre al terminar. Se guarda el UserId ademas del correo para poder rehacer
   esa atribucion si algun dia hiciera falta.

   2. uso_diario.ruta

   `Module` no sirve para decir que pantalla se uso: la mitad de sus valores son
   GUIDs (`a1a5d0ea-edb4-4166-f3f8-08ddced0ef`). `Route` si trae el endpoint, y
   normalizado -los identificadores sustituidos por {id}- quedan unas pocas
   decenas de rutas legibles. Sin esto, el panel "que pantallas se usan" del
   tablero ensenaria GUIDs.

   SE REHACEN LAS TABLAS en vez de alterarlas: entre las dos tienen 113 filas de
   un solo dia, que se vuelven a generar en la proxima corrida. Alterar con datos
   dentro seria mas delicado y no gana nada.
   ============================================================================ */

USE Sidon_Uso;
GO

/* Si alguien ya cargo el historico, estas tablas valen mucho mas que 113 filas:
   el origen puede haberse limpiado y no habria de donde volver a sacarlas. */
IF (SELECT COUNT(*) FROM dbo.uso_diario) > 500
    OR (SELECT COUNT(*) FROM dbo.logins) > 200
BEGIN
    RAISERROR(N'Las tablas tienen datos que parecen el historico. No se borran: revisar a mano.', 16, 1);
    RETURN;
END
GO

DROP TABLE IF EXISTS dbo.uso_diario;
GO

CREATE TABLE dbo.uso_diario (
    fecha        DATE          NOT NULL,
    usuario      NVARCHAR(200) NOT NULL,
    modulo       NVARCHAR(200) NOT NULL,
    /* La ruta sin sus identificadores: GET /api/productionLines/{id} */
    ruta         NVARCHAR(300) NOT NULL,
    peticiones   INT           NOT NULL,
    primera_utc  DATETIME2(0)  NOT NULL,
    ultima_utc   DATETIME2(0)  NOT NULL,
    ips          INT           NOT NULL,
    CONSTRAINT PK_uso_diario PRIMARY KEY (fecha, usuario, modulo, ruta)
);
GO

CREATE INDEX IX_uso_diario_ruta ON dbo.uso_diario (ruta) INCLUDE (peticiones);
GO

DROP TABLE IF EXISTS dbo.logins;
GO

CREATE TABLE dbo.logins (
    registro_id  UNIQUEIDENTIFIER NOT NULL,
    /* Puede venir nulo: hay logins donde el sistema no identifico a nadie. */
    usuario_id   UNIQUEIDENTIFIER NULL,
    usuario      NVARCHAR(200)    NOT NULL,
    momento_utc  DATETIME2(0)     NOT NULL,
    fecha        DATE             NOT NULL,
    hora_local   TIME(0)          NOT NULL,
    ip           NVARCHAR(64)     NULL,
    codigo       INT              NULL,
    exitoso      BIT              NOT NULL,
    identificado BIT              NOT NULL,
    CONSTRAINT PK_logins PRIMARY KEY (registro_id)
);
GO

CREATE INDEX IX_logins_fecha ON dbo.logins (fecha) INCLUDE (usuario, exitoso);
GO

CREATE INDEX IX_logins_usuario ON dbo.logins (usuario, momento_utc);
GO

/* La marca de control se borra tambien: si no, la proxima corrida creeria que el
   30 de septiembre ya esta hecho y no lo volveria a escribir. */
DELETE FROM dbo.etl_control WHERE proceso = N'uso_sidon';
GO

PRINT 'uso_diario y logins rehechas. El control quedo limpio para volver a correr.';
GO
