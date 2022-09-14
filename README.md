# Matriz-Origen-Destino-Transporte-Publico

En este repositorio se encuentran los resultados de la matriz OD en base a datos SUBE para la Región Metropolitana de Buenos Aires (RMBA) para un día miércoles de noviembre representativo del año 2019, como así también el dataset de transacciones SUBE y los procedimientos para obtenerla mediante scripts de PostgresSQL + PostGIS. 

Los datos SUBE fueron obtenidos mediante un pedido de acceso a la información pública (expediente *EX - 2020 - 32945006 - DNAIP#AAIP*). A criterio del área de incumbencia técnica, para preservar la privacidad de los usuarios se omitió la identificación de la tarjeta SUBE, enmascarando esa información (junto a los números de interno de los colectivos). Asimismo, para mayor resguardo de información personal se procedió a agregar espacial y temporalmente la información. No se ofrecen datos de los minutos en los que ocurrió la transacción (sólo la hora) y las coordenadas han sido truncadas a tres decimales (esto ofrece un margen de error de aproximadamente 100m). La información en el archivo transacciones intenta presentar el dataset original tal cual fue provisto, haciendo solo modificaciones al esquema de datos para ofrecerlo de la manera mas eficiente posible. 

Esta información, como otras tablas insumo necesarias para el proceso, se encuentran en el directorio `data/` del repositorio, siendo `transacciones.csv` el insumo principal con las transacciones SUBE otorgadas en el pedido de información pública. En el directorio `resultados/` encontrarán todos los resultados finales, con las tablas correspondientes a las **etapas**, **viajes** y **tarjetas**. Finalmente, en el directorio `queries/` encontrarán los scripts para obtener estos resultados a partir de los datos. 

El procesamiento está hecho en PostgresSQL + PostGIS junto a la infraestructura de celdas hexagonales jerarquicas [H3 de Uber](https://eng.uber.com/h3/), para los que se utilizaron los bindings para Postgres desarrollados por [bytesandbrains](https://github.com/bytesandbrains) por [aqui](https://github.com/bytesandbrains/h3-pg). Las queries toman como principal insumo los archivos `transacciones.csv`, `paradas.csv` y `lineas_ramales.csv`, procesan esa información y elaboran las correspondientes matrices y tablas conexas. Para más detalles sobre la metodología pueden leer el informe final presente en este repositorio.


## Guía de usuario

Para quienes quieran utilizar los microdatos de la matriz OD pueden simplemente utilizar los datos presentes en `resultados/`. La información relativa a las paradas, indices h3, lineas y ramales pueden encontrarla en `data/`, para hacer los joins mediante los ids. Por ejemplo, para georeferenciar cada origen y destino deberan unir con la tabla paradas mediante el `id`. A su vez, para obtener información contextual sobre cada indice `h3`, se debera unir el indice `h3` de la tabla `paradas` con el de la tabla `indices_h3`.

Para quienes quieran reproducir y/o modificar algunos de los proedimientos del trabajo realizado deberán:

1. Instalar [Postgres](https://www.postgresql.org/) + [Postgis](https://postgis.net/) +  [h3-pg](https://github.com/bytesandbrains/h3-pg).

2. Crear una base de datos. 

3. Abrir el script `1_crear_tablas_principales.sql`, modificar `[PATH]` por la ruta donde se haya clonado el repositorio y ejecutarlo.

4. Correr los scripts en el siguiente orden: `2_construir_etapas.sql`, `3_imputar_destinos.sql` y `4_construir_tablas_viajes_tarjetas.sql`.


## Licencia

Copyright© 2022. Banco Interamericano de Desarrollo ("BID"). Uso autorizado. [AM-331-A3](/LICENSE.md)


## Autores

Felipe González ([@alephcero](https://github.com/alephcero/)) 
Sebastián Anaposlky([@sanapolsky](https://github.com/sanapolsky/))

