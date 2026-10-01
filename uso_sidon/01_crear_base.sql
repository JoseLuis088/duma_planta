/* ============================================================================
   Sidon_Uso: el archivo del uso de Sidon Industrial.

   QUE GUARDA Y POR QUE AQUI. La bitacora de Sidon vive en dbo.SystemLogs, en su
   Azure SQL, y hay indicios de que se purga: las estadisticas de la tabla
   registraron 2,731,108 filas en marzo y hoy hay 1,986,171. Si eso es una
   politica de retencion, esta base es el unico sitio donde esos meses van a
   seguir existiendo. De ahi la regla que gobierna todo el diseno:

       ESTAS TABLAS SE ESCRIBEN HACIA ADELANTE Y NO SE RECONSTRUYEN NUNCA.

   Reconstruirlas desde el origen el dia de manana borraria justo lo que el
   origen ya no tiene. El ETL solo toca los dias de la ventana que procesa.

   POR QUE BASE APARTE Y NO DENTRO DE Duma_Planta. El archivo de uso es de larga
   vida y el historial de conversaciones de Duma no; y asi se puede dar acceso
   al uso sin exponer las conversaciones de nadie.

   POR QUE LAS COLUMNAS DE TEXTO SON NVARCHAR(200). Es el error exacto que tiene
   la tabla de origen: alla son nvarchar(max), que SQL Server no puede indexar,
   y por eso una consulta tan simple como "que modulos se usaron ayer" tarda mas
   de quince minutos sobre dos millones de filas. Aqui se pueden indexar y el
   informe responde al instante.

   ANTES DE CORRER ESTE ARCHIVO: hay que cambiar <<CLAVE_DEL_ETL>> por una clave
   de verdad, mas abajo. No la escribas en el repositorio.

   DESPUES DE CORRERLO: meter Sidon_Uso al respaldo diario, como se explica en
   el diseno. Un archivo sin respaldo no es un archivo.
   ============================================================================ */

USE master;
GO

IF DB_ID(N'Sidon_Uso') IS NULL
BEGIN
    CREATE DATABASE Sidon_Uso;
END
GO

/* SIMPLE porque es lo que usan las demas bases de esta VM y porque el contenido
   se puede volver a generar desde el origen MIENTRAS el origen lo conserve. El
   respaldo diario es lo que cubre lo que ya no se puede regenerar. */
ALTER DATABASE Sidon_Uso SET RECOVERY SIMPLE;
GO

/* ---------------------------------------------------------------------------
   El usuario del ETL.

   No se usa `sa`, por la misma razon por la que no lo usa el respaldo de
   Sidon_Ecosat: esa credencial abre las nueve bases de una VM que comparte diez
   proyectos de otros clientes. Este usuario solo puede leer y escribir aqui.
   --------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'sidon_uso_etl')
BEGIN
    CREATE LOGIN sidon_uso_etl WITH PASSWORD = N'<<CLAVE_DEL_ETL>>',
        CHECK_POLICY = ON, DEFAULT_DATABASE = Sidon_Uso;
END
GO

USE Sidon_Uso;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'sidon_uso_etl')
BEGIN
    CREATE USER sidon_uso_etl FOR LOGIN sidon_uso_etl;
    ALTER ROLE db_datareader ADD MEMBER sidon_uso_etl;
    ALTER ROLE db_datawriter ADD MEMBER sidon_uso_etl;
END
GO

/* ---------------------------------------------------------------------------
   Las tablas.

   `fecha` es siempre el dia LOCAL de planta, porque es como se pregunta ("quien
   entro el martes"). Los instantes se guardan ademas en UTC, que es como viene
   el origen: si manana resulta que la conversion estaba mal, se recalcula sin
   volver a tocar la fuente. Sin el UTC esa correccion seria imposible.
   --------------------------------------------------------------------------- */

/* Un renglon por dia, usuario y modulo. Es la tabla que contesta casi todo. */
IF OBJECT_ID(N'dbo.uso_diario') IS NULL
CREATE TABLE dbo.uso_diario (
    fecha        DATE          NOT NULL,
    usuario      NVARCHAR(200) NOT NULL,
    modulo       NVARCHAR(200) NOT NULL,
    peticiones   INT           NOT NULL,
    primera_utc  DATETIME2(0)  NOT NULL,
    ultima_utc   DATETIME2(0)  NOT NULL,
    ips          INT           NOT NULL,
    CONSTRAINT PK_uso_diario PRIMARY KEY (fecha, usuario, modulo)
);
GO

/* Cada entrada al sistema, una por fila. Es el dato que se pidio: quien entra y
   a que hora. No se infiere de nada -el origen lo registra con Module='login'
   y Route='POST /api/users/login'.

   `codigo` sale del StatusCode que viene en el Body de esa fila. Un codigo
   distinto de 200 es un intento fallido, que dice tanto del uso como los que
   funcionan y conviene poder contarlos aparte. */
IF OBJECT_ID(N'dbo.logins') IS NULL
CREATE TABLE dbo.logins (
    usuario      NVARCHAR(200) NOT NULL,
    momento_utc  DATETIME2(0)  NOT NULL,
    fecha        DATE          NOT NULL,
    hora_local   TIME(0)       NOT NULL,
    ip           NVARCHAR(64)  NULL,
    codigo       INT           NULL,
    exitoso      BIT           NOT NULL,
    CONSTRAINT PK_logins PRIMARY KEY (usuario, momento_utc)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_logins_fecha')
CREATE INDEX IX_logins_fecha ON dbo.logins (fecha) INCLUDE (usuario, exitoso);
GO

/* Sesiones: tramos de actividad continua, cortados donde hay mas de 30 minutos
   sin pedir nada. Miden cuanto duro la visita, que el login solo no dice.

   `abrio_sesion` distingue las que empiezan con un login de verdad de las que
   empiezan a media actividad -por ejemplo porque la sesion venia del dia
   anterior, o porque el usuario ya tenia la pagina abierta. */
IF OBJECT_ID(N'dbo.sesiones') IS NULL
CREATE TABLE dbo.sesiones (
    usuario      NVARCHAR(200) NOT NULL,
    inicio_utc   DATETIME2(0)  NOT NULL,
    fin_utc      DATETIME2(0)  NOT NULL,
    fecha        DATE          NOT NULL,
    minutos      INT           NOT NULL,
    peticiones   INT           NOT NULL,
    modulos      INT           NOT NULL,
    abrio_sesion BIT           NOT NULL,
    CONSTRAINT PK_sesiones PRIMARY KEY (usuario, inicio_utc)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_sesiones_fecha')
CREATE INDEX IX_sesiones_fecha ON dbo.sesiones (fecha) INCLUDE (usuario, minutos);
GO

/* Quien es persona y quien es maquina. Vive como dato y no escondido en un `if`
   del codigo: manana puede aparecer otra cuenta de servicio y hay que poder
   corregirlo sin desplegar nada. El ETL da de alta como persona cualquier
   cuenta nueva que no conozca, y la marca en `nota` para que alguien la mire. */
IF OBJECT_ID(N'dbo.cuentas') IS NULL
CREATE TABLE dbo.cuentas (
    usuario      NVARCHAR(200) NOT NULL PRIMARY KEY,
    es_persona   BIT           NOT NULL,
    nota         NVARCHAR(200) NULL,
    visto_desde  DATE          NULL,
    visto_hasta  DATE          NULL
);
GO

/* Hasta donde llego el ETL y como le fue. Sin esto no sabria por donde seguir,
   y una corrida fallida se perderia sin dejar rastro. */
IF OBJECT_ID(N'dbo.etl_control') IS NULL
CREATE TABLE dbo.etl_control (
    proceso              NVARCHAR(50)  NOT NULL PRIMARY KEY,
    procesado_hasta_utc  DATETIME2(0)  NOT NULL,
    corrida_utc          DATETIME2(0)  NOT NULL,
    filas_leidas         BIGINT        NOT NULL,
    segundos             INT           NOT NULL,
    resultado            NVARCHAR(400) NOT NULL
);
GO

/* Las dos cuentas de servicio que ya se conocen. Generan casi todo el trafico
   -se les ve pegandole a /api/productionLines en bucle- y salen de IPs de Azure
   (20.150.x.x), no de la red de Bafar. Sin separarlas, cualquier metrica de uso
   queda inflada por trafico de maquinas. */
MERGE dbo.cuentas AS d
USING (VALUES
    (N'monitor@bafar.com.mx', 0, N'Cuenta de sistema: monitoreo automatico'),
    (N'sidon@bafar.com',      0, N'Cuenta de sistema: procesos internos')
) AS o (usuario, es_persona, nota)
ON d.usuario = o.usuario
WHEN NOT MATCHED THEN
    INSERT (usuario, es_persona, nota) VALUES (o.usuario, o.es_persona, o.nota);
GO

PRINT 'Sidon_Uso lista. Falta: cambiar la clave del login y meterla al respaldo diario.';
GO
