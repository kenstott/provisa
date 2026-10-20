# Crear una vista bitemporal {: #create-a-bitemporal-view }

Una vista bitemporal guarda las dos cosas: cuándo ocurrió algo y cuándo se registró. Así puede leerla a una fecha, o tal como estaba registrada en una fecha, y una corrección posterior nunca oculta lo que se sabía antes de ella.

Una vista se vuelve bitemporal al asignar tres de sus columnas:

- **Clave de negocio**: la columna, o columnas, que identifican el mismo registro entre versiones, como `transaction_id`. Un pago corregido sigue siendo el mismo pago, así que ambas versiones llevan la misma clave.
- **Tiempo válido**: la columna con la fecha que su negocio da al registro, como cuándo se acordó, se contabilizó o entró en vigor.
- **Tiempo de registro**: la columna con cuándo se introdujo o cambió esta versión, como `updated_at`.

El tiempo de registro se llama tiempo de transacción en la literatura de bases de datos temporales y tiempo del sistema en SQL:2011; el tiempo válido se llama tiempo de aplicación en SQL:2011 o fecha efectiva.

Los tres valores asignan columnas que su fuente ya tiene. Provisa decide cómo se guardan las versiones; no hay un modo de almacenamiento que elegir.

## Tres ejemplos {: #three-examples }

| Ejemplo | Tiempo válido | Tiempo de registro |
| ------- | ------------- | ------------------ |
| Un pago de 100 del 3 de oct. se corrige a 120 el 5 de oct. | 3 de oct., que la corrección conserva. | 3 de oct. para la primera versión; 5 de oct. para la corrección. |
| Una medición de campo tomada el 3 de oct. a las 14:10 se teclea el 6 de oct. Una errata en ella se corrige el 7 de oct. | 3 de oct. 14:10, que la corrección conserva. | 6 de oct.; la corrección recibe el 7 de oct. |
| Una operación se contabiliza al ejecutarse, fecha de operación 3 de oct., a las 10:02. El 4 de oct. su cantidad resulta errónea y se rectifica. | Fecha de operación 3 de oct., que la rectificación conserva. | 3 de oct. 10:02; la rectificación recibe el 4 de oct. |

Leída tal como se informó el 3 de oct., la operación muestra la cantidad errónea. Leída según lo que se sabe hoy, muestra la corregida.

## Crear la vista {: #create-the-view }

Una vista que guarda historial está materializada, para que sus versiones se almacenen.

1. Abra **Views** y haga clic en **+ View**.
2. Defina la vista en cualquiera de las dos pestañas:
    - **Metrics**: ponga nombre a la vista, elija su dominio, métricas y dimensiones y haga clic en **Continue**.
    - **SQL**: abra el editor SQL, escriba y ejecute el `SELECT`, haga clic en **+ View** en el editor, ponga nombre a la vista y haga clic en **Continue**.
3. Los ajustes de la vista se abren en las mismas secciones que su editor. En **Load and timeliness**, marque **Materialized View**. Aparece la sección **Time Travel**.
4. Ponga **Keep history** en **Bitemporal**.
5. Elija las columnas de **Business key**, **Valid time** y **Recorded time**.
6. Ajuste lo que necesite en las demás secciones y haga clic en **Save**. Hasta entonces no se registra nada.

Las tres se declaran juntas o ninguna. Provisa rechaza una vista que nombra solo algunas e indica cuáles faltan.

Una tabla recibe las mismas secciones al registrarla: elíjala en **+ Table**, haga clic en **Continue** y su sección Time Travel está en **Load and timeliness**. Un modelo recibe la sección al crearlo (**+ Model**). En un modelo, su clave es la clave de negocio, así que la sección solo pide los dos tiempos.

## Una tabla que se actualiza en su lugar {: #a-table-updated-in-place }

Algunas tablas guardan cada versión como una fila propia, de modo que una corrección añade una fila con la misma clave de negocio. Otras mantienen una fila por clave y la sobrescriben, así que su clave de negocio es su clave primaria. Para una tabla del segundo tipo, Provisa guarda las versiones por sí mismo en un registro de versiones:

- Cada captura lee las filas registradas desde la última hora de registro que contiene el registro, y añade las que cambiaron.
- El retraso permitido del registro amplía esa ventana, de modo que una fila confirmada tarde con una hora de registro anterior se sigue leyendo.
- Una captura no ve una fila que la fuente eliminó. **Full capture** vuelve a leer toda la tabla y marca cada fila eliminada como desaparecida.
- El registro empieza en su primera captura: antes de ella, cada fila solo tiene la versión que tenía entonces.

El registro aparece en **Cache**, en la pestaña **Materialized Store**, como **Version log of** la tabla, con su botón **Full capture**. Una lectura simple de la tabla lee la tabla; una lectura a una fecha, o tal como se registró en una fecha, lee su registro.

## Leerla {: #read-it }

La vista aparece en la lista **Views** con la insignia **HISTORY**. Su panel de detalle indica que guarda historial y enlaza al explorador SQL con la vista seleccionada. Ponga los selectores **As of** y **As recorded at** del explorador en los momentos que quiera y ejecute la consulta.

Los mismos tiempos llegan a todas las demás superficies. Una sesión pgwire los fija una vez para cada sentencia que ejecuta:

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

Las cabeceras HTTP, los argumentos de `resolve` y los parámetros REST están en [Vistas con historial](configuration.md#views-with-history-time-travel).

## Cambiarla más tarde {: #change-it-later }

El editor muestra la sección Time Travel en la pestaña **Load and timeliness** de la vista. Una vez construida la vista, sus versiones quedan registradas bajo su clave de negocio y su tiempo válido, así que el editor ya no ofrece esos dos ni el interruptor que desactiva Time Travel. El tiempo de registro aún puede cambiar. Para organizar el historial con otra clave, cree una vista nueva.
