/* ============================================================================
   Arregla la llave de dbo.logins.

   QUE PASO. La primera corrida real leyo 1,976,334 filas sin problema y murio al
   escribir:

     Violation of PRIMARY KEY constraint 'PK_logins'
     La llave duplicada es (, 2026-08-20 23:06:18)

   El usuario de esa llave esta VACIO. En el origen hay filas de login sin correo
   -lo mas probable, intentos donde ni se llego a identificar a la persona- y dos
   de ellas cayeron en el mismo segundo.

   POR QUE ESTABA MAL. La llave era (usuario, momento_utc), que da por hecho que
   una persona no entra dos veces en el mismo segundo. Para una persona de verdad
   es razonable; para un usuario vacio, donde todas las filas anonimas se ven como
   la misma "persona", se cae enseguida.

   LA LLAVE BUENA NO HAY QUE INVENTARLA. dbo.SystemLogs ya tiene una por fila,
   RegisterId, unica por definicion porque es su propia llave primaria. Traerla
   resuelve el choque y ademas deja cada login rastreable hasta la fila exacta que
   lo origino, que antes no se podia.

   SE PUEDE BORRAR Y REHACER LA TABLA porque esta vacia: la transaccion que fallo
   no dejo una sola fila. Comprobado antes de escribir esto.
   ============================================================================ */

USE Sidon_Uso;
GO

IF OBJECT_ID(N'dbo.logins') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.logins)
    BEGIN
        /* Si alguien ya la lleno, este script no es seguro: borrarla perderia
           datos que quiza no se puedan volver a leer del origen. Mejor parar. */
        RAISERROR(N'dbo.logins tiene filas. No se borra: revisar a mano.', 16, 1);
        RETURN;
    END
    DROP TABLE dbo.logins;
END
GO

CREATE TABLE dbo.logins (
    /* La llave es la de la fila que lo origino en dbo.SystemLogs. Unica de por
       si, y permite volver al origen cuando un numero no cuadre. */
    registro_id  UNIQUEIDENTIFIER NOT NULL,
    usuario      NVARCHAR(200)    NOT NULL,
    momento_utc  DATETIME2(0)     NOT NULL,
    fecha        DATE             NOT NULL,
    hora_local   TIME(0)          NOT NULL,
    ip           NVARCHAR(64)     NULL,
    codigo       INT              NULL,
    exitoso      BIT              NOT NULL,
    /* Las filas sin correo se guardan como '(sin identificar)' en vez de como
       cadena vacia: asi se ven en el informe como lo que son -accesos que no se
       pudieron atribuir a nadie- y no se confunden con una persona real. */
    identificado BIT              NOT NULL,
    CONSTRAINT PK_logins PRIMARY KEY (registro_id)
);
GO

CREATE INDEX IX_logins_fecha ON dbo.logins (fecha) INCLUDE (usuario, exitoso);
GO

CREATE INDEX IX_logins_usuario ON dbo.logins (usuario, momento_utc);
GO

PRINT 'dbo.logins rehecha con registro_id como llave.';
GO
