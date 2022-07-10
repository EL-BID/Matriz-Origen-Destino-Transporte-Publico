/*
 Este script crea las tablas e insumos fundamentales para el proceso de imputacion de destinos
 Para continuar deberá reemplazar PATH en todas las sentencias COPY 
 */

-- Axtivar extensiones para postgis y los bindings de h3
CREATE EXTENSION if not EXISTS postgis;
CREATE EXTENSION if not exists h3;

-- Tablas de paradas de transporte publico de AMBA
CREATE TABLE paradas (
	id serial PRIMARY KEY,
	id_linea numeric,
	id_ramal numeric ,
	longitud numeric,
	latitud numeric,
	nombre_estacion text,
	h3 h3index
);

COPY paradas(id,id_linea,id_ramal,nombre_estacion,longitud,latitud,h3)
FROM '[PATH]/Matriz-Origen-Destino-Transporte-Publico/data/paradas.csv'
CSV HEADER;

-- Crear punto de parada, buffer e index espacial
SELECT AddGeometryColumn ('public','paradas','geom',4326,'POLYGON',2);

update paradas 
set geom = ST_Buffer(ST_SetSRID(ST_MakePoint(longitud, latitud),4326),0.03);

CREATE INDEX paradas_geom_idx
  ON paradas
  USING GIST (geom);
 

CREATE INDEX idx_paradas_linea
ON paradas(id_linea);

CREATE INDEX idx_paradas_ramal
ON paradas(id_ramal);

-- Tabla de lineas y ramales de transporte publico de AMBA
CREATE TABLE lineas_ramales (
	id_linea numeric,
	linea text,
	modo text,
	id_ramal numeric,
	ramal_corto text,
	ramal text,
	provincia text,
	municipio text
);

COPY lineas_ramales(id_linea,linea,modo,id_ramal,ramal_corto,ramal,provincia,municipio)
FROM '[PATH]/Matriz-Origen-Destino-Transporte-Publico/data/lineas_ramales.csv'
CSV HEADER;


-- Tabla con transacciones sube en formato original
CREATE TABLE trx (
	id serial PRIMARY KEY,
	id_tarjeta bigint,
	modo text,
	lat numeric,
	lon numeric,
	sexo text,
	interno_bus numeric,
	tipo_trx_tren text,
	etapa_red_sube numeric,
	id_linea numeric,
	id_ramal numeric,
	id_tarifa numeric,
	hora numeric
);

COPY trx(id,id_tarjeta, modo, lat, lon, sexo, interno_bus,
       tipo_trx_tren, etapa_red_sube,id_linea, id_ramal, id_tarifa, hora)
FROM '[PATH]/Matriz-Origen-Destino-Transporte-Publico/data/transacciones.csv'
CSV HEADER;



CREATE TABLE indices_h3 (
	h3 h3index  PRIMARY KEY,
	radio_2010 char(9),
	depto_2010 char(5),
	departamento text,
	provincia text
);


COPY indices_h3(h3,radio_2010,depto_2010,departamento,provincia)
FROM '[PATH]/Matriz-Origen-Destino-Transporte-Publico/data/indices_h3.csv'
CSV HEADER;
