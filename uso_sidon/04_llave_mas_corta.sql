/* ============================================================================
   La llave de uso_diario no cabia.

   Al crearla, SQL Server aviso:

     Warning! The maximum key length for a clustered index is 900 bytes.
     The index 'PK_uso_diario' has maximum length of 1403 bytes.
     For some combination of large values, the insert/update operation will fail.

   La cuenta: fecha 3 + usuario 400 + modulo 400 + ruta 600 = 1403, y el limite
   son 900. La tabla se CREA igual -el aviso no la impide- pero la insercion
   revienta en cuanto una fila junte valores largos. Habria pasado a mitad del
   relleno historico, despues de cincuenta minutos de lectura.

   LA SOLUCION ES ACOTAR, NO PARTIR LA LLAVE. Los anchos que habia puesto eran
   generosos sin razon: el correo mas largo que hemos visto mide 22 caracteres,
   un GUID de modulo 36, y la ruta normalizada mas larga ronda los 60. Con estos
   anchos sigue sobrando sitio de sobra y la llave baja a 803 bytes.

     fecha      3
     usuario  200   (NVARCHAR 100)
     modulo   200
     ruta     400
            -----
              803   cabe

   Las tablas estan vacias -la corrida anterior se borro con el script 03- asi
   que se rehacen sin perder nada.
   ============================================================================ */

USE Sidon_Uso;
GO

IF (SELECT COUNT(*) FROM dbo.uso_diario) > 500
BEGIN
    RAISERROR(N'uso_diario tiene datos. No se borra: revisar a mano.', 16, 1);
    RETURN;
END
GO

DROP TABLE IF EXISTS dbo.uso_diario;
GO

CREATE TABLE dbo.uso_diario (
    fecha        DATE          NOT NULL,
    usuario      NVARCHAR(100) NOT NULL,
    modulo       NVARCHAR(100) NOT NULL,
    ruta         NVARCHAR(200) NOT NULL,
    peticiones   INT           NOT NULL,
    primera_utc  DATETIME2(0)  NOT NULL,
    ultima_utc   DATETIME2(0)  NOT NULL,
    ips          INT           NOT NULL,
    CONSTRAINT PK_uso_diario PRIMARY KEY (fecha, usuario, modulo, ruta)
);
GO

CREATE INDEX IX_uso_diario_ruta ON dbo.uso_diario (ruta) INCLUDE (peticiones);
GO

/* Por consistencia: en logins la llave es el GUID del origen, asi que el ancho
   del correo no afecta a ningun indice. Se acota igual para que las dos tablas
   digan lo mismo sobre lo que cabe en un correo. */
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE object_id = OBJECT_ID(N'dbo.logins')
             AND name = N'usuario' AND max_length > 200)
BEGIN
    DROP INDEX IX_logins_usuario ON dbo.logins;
    ALTER TABLE dbo.logins ALTER COLUMN usuario NVARCHAR(100) NOT NULL;
    CREATE INDEX IX_logins_usuario ON dbo.logins (usuario, momento_utc);
END
GO

DELETE FROM dbo.etl_control WHERE proceso = N'uso_sidon';
GO

PRINT 'Llave de uso_diario en 803 bytes. Sin avisos.';
GO
