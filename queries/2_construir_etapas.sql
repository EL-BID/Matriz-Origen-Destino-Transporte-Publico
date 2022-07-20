/*
Este script crea la tabla de etapas, reconstruyendo la cadena de viejas y etapas con sus 
correspondientes ids. 
*/

-- Crear tabla etapas con transacciones con id tarjeta y eliminar checkouts sin checkin
create table etapas as ( 
	select * 
	from trx t
	where t.id_tarjeta is not null
	and (
		tipo_trx_tren <> 'CHECK OUT SIN CHECKIN' or tipo_trx_tren is null
		)
);


CREATE INDEX idx_etapas 
ON etapas(id);

CREATE INDEX idx_etapas_tarjeta 
ON etapas(id_tarjeta);
 

--------------------------------------------------------------------------------------------------------------------------------
-- CREAR ID ETAPA Y ID VIAJE
--------------------------------------------------------------------------------------------------------------------------------

/*
Crear tabla insumo de ids etapas viajes para poder crear los ids
Para cada tarjeta, ordena por hora, etapa red sube y tipo de transaccion
crea un id_etapa con un incremental para las transacciones de usos (no checkouts)
*/
create table ids_etapas_viajes as (
	select id, id_tarjeta, hora, tipo_trx_tren, etapa_red_sube,  
	sum(
		CASE WHEN tipo_trx_tren <> 'CHECK OUT' or tipo_trx_tren is null THEN 1 ELSE 0 END)
			OVER (PARTITION BY id_tarjeta ORDER BY hora, etapa_red_sube, tipo_trx_tren) AS id_etapa
	FROM etapas
	order by id_tarjeta, hora, etapa_red_sube, tipo_trx_tren 
);


-- Agregar un id viaje temporal
alter table ids_etapas_viajes
add column id_viaje_temp int8;

-- Crear un id de viaje temporal que no es secuencial
update ids_etapas_viajes e
set id_viaje_temp = id_etapa - etapa_red_sube;

/*
Al no contar con un timestamp que permita ordenar en el tiempo las transacciones y solo tener hora y etapa red sube
hay un problema para ordenar en el tiempo y con los ids cuando a un Check Out le sigue un uso dentro de la misma hora.
Algunas transacciones checkout quedan identificadas con un id_etapa correspondiente a la siguiente etapa y con un id_viaje
propio, sin que ninguno de los dos coincida con la etapa y viaje correspondiente. 

Un ejemplo es el id_tarjeta = 37035823 en las dos transacciones de las 17hs, donde el check out deberia coincidir
con el id de la etapa y el viaje del checkin. Tambien la tarjeta 5539503909, con dos suceso de este tipo. Las queries siguientes procuran solucionarlo   


Sin embrago para subte, cuando una misma tarjeta hace dos transacciones en el mismo ramal del subte (por ej linea H) a la misma hora, al no haber integracion
tarifaria marcada en etapa_red_sube no hay forma de ordenar en el tiempo las transacciones dentro de esa hora. Por ej tarjeta 7239578027. 
Se podría buscar un criterio de minimizacion de distancias con respecto a las transacciones anteriores o posteriores para ordenar, cambiar 
para cada hora de las transacciones en subte en la misma hora por la hora de la transaccion mas cercana en el espacio y reasignar el id etapa.
La tarjeta 7239578027 es un caso donde no hay transaccion posterior y deberia usarse solo la anterior.
*/

-----------------------------------------------------------------------------------------------------
-- Solucion checkout seguido de transaccion en la misma hora
-----------------------------------------------------------------------------------------------------

-- Crear una tabla con los id viaje 
create table viajes_solo_checkout as (
	select id_tarjeta, id_viaje_temp 
	from (
		select id_tarjeta, id_viaje_temp, count(*) as trxs, sum(case when tipo_trx_tren = 'CHECK OUT' then 1 else 0 end) as checksouts
		from ids_etapas_viajes
		group by id_tarjeta, id_viaje_temp) tabla
	where checksouts = trxs
);

alter table viajes_solo_checkout
add column viaje_target numeric,
add column etapa_target  numeric;



--Para todos los id viajes que sean solo checkout, se reemplaza su id por el id del viaje anterior (el del checkin) 
with viajes_anteriores_al_checkout as (
	select distinct on (iev.id_tarjeta,vsc.id_viaje_temp) iev.id_tarjeta, vsc.id_viaje_temp, iev.id_viaje_temp as viaje_target   
	from ids_etapas_viajes iev, viajes_solo_checkout vsc
	where iev.id_tarjeta = vsc.id_tarjeta 
	and vsc.id_viaje_temp > iev.id_viaje_temp
	order by id_tarjeta,id_viaje_temp desc,viaje_target desc
)
update viajes_solo_checkout vsc
set viaje_target = vt.viaje_target 
from viajes_anteriores_al_checkout vt 
where vsc.id_tarjeta = vt.id_tarjeta
and vsc.id_viaje_temp = vt.id_viaje_temp;

-- Dentro de cada tarjeta y viaje, se reemplaza el id_etapa de las transacciones solo con checkout por el id de la etapa del checkin 
with etapas_anteriores_al_checkout as (
	select distinct iev.id_tarjeta, vsc.id_viaje_temp, id_etapa as etapa_target
	from ids_etapas_viajes iev, viajes_solo_checkout vsc
	where iev.id_tarjeta = vsc.id_tarjeta 
	and vsc.id_viaje_temp > iev.id_viaje_temp
	and tipo_trx_tren = 'CHECK IN'),
etapas_target as (
	select id_tarjeta, id_viaje_temp, max(etapa_target) as etapa_target  
	from etapas_anteriores_al_checkout
	group by id_tarjeta, id_viaje_temp 
	order by id_tarjeta,id_viaje_temp
)
update viajes_solo_checkout vsc
set etapa_target = et.etapa_target 
from etapas_target et 
where vsc.id_tarjeta = et.id_tarjeta
and vsc.id_viaje_temp = et.id_viaje_temp;


-- Actualizar la tabla de ids con los nuevos correspondientes a los checkouts mal asignados
update ids_etapas_viajes iev 
set 
	id_viaje_temp = vsc.viaje_target ,
	id_etapa = vsc.etapa_target 
from viajes_solo_checkout vsc
where iev.id_tarjeta = vsc.id_tarjeta 
and iev.id_viaje_temp = vsc.id_viaje_temp; 


-- Eliminar tabla insumo
drop table viajes_solo_checkout;

/*
El haber elminado ids de viajes que eran solo checkout, deja un id viaje con hiatos en la secuencia
Se reemplazan por una secuencia para cada tarjeta
*/

alter table ids_etapas_viajes
add column id_viaje numeric;


-- Sumar nuevos ids a la tabla ids
update ids_etapas_viajes ev
set id_viaje = v.id_viaje 
from (
	select *,  row_number () over (partition by id_tarjeta order by id_tarjeta, id_viaje_temp) id_viaje
	from (
		select distinct id_tarjeta, id_viaje_temp
		from ids_etapas_viajes
		order by id_tarjeta, id_viaje_temp
	) viajes_temp
) v
where ev.id_tarjeta = v.id_tarjeta 
and ev.id_viaje_temp = v.id_viaje_temp; 


-- Agregar columnas de ids etapas y viejas a tabla etapas 
alter table etapas
add column id_etapa numeric,
add column id_viaje numeric;

CREATE INDEX idx_ids_etapas_viajes 
ON ids_etapas_viajes(id);

-- agregar id_etapa e id_viaje a tabla de etapas
update etapas e
set id_etapa = t.id_etapa,
id_viaje = t.id_viaje
from ids_etapas_viajes t
where e.id = t.id;

drop table ids_etapas_viajes;

/*
Eliminar etapas con problemas de geolocalizacion
*/
-- asignar h3 
alter table etapas
add column h3 h3index;

update etapas  
set h3 =  h3_geo_to_h3(POINT(lon,lat), 10);



-- Borrar toda las etapas con transacciones fuera de un rango logico de latong  o sin latlong
delete from etapas
where lat < -36
	or lat > -33
	or lon <-61
	or lon > -57
	or h3 is null;

-- Borrar tarjetas simple transaccion.  
DELETE FROM etapas e
USING (	select id_tarjeta, count(*)
	from etapas 
	group by id_tarjeta
	having count(*) = 1) as u
WHERE e.id_tarjeta = u.id_tarjeta;



/*
 Asignar el ide linea en base al id ramal de acuerdo a la tabla linea ramales
 El id linea que viene en las transacciones sube no coincide con nuestra definicion de linea.
 Por ejemplo, el subte no es una unica linea.
*/
UPDATE etapas
SET id_linea = lineas_ramales.id_linea
FROM lineas_ramales
WHERE etapas.id_ramal = lineas_ramales.id_ramal;


/*
 Una misma tarjeta puede usarse dos veces en el mismo colectivo o molinete, ya sea porque viajan dos personas
 o por error. Decidimos eliminar uno de esos registros. Cada criterio de duplicado varia segun modo 
 */

-- COLECTIVOS
-- borrar casos que repiten tarjeta, hora, etapa e interno en bus
with dups as (
	select e.id, e.id_tarjeta,e.id_linea, e.interno_bus, e.hora, e.etapa_red_sube 
	from etapas e,
	(
		select id_tarjeta,id_linea, interno_bus, hora, etapa_red_sube, count(*)
		from etapas
		where modo = 'COL'
		group by id_tarjeta,id_linea, interno_bus, hora, etapa_red_sube
		having count(*) > 1
	) d 
	where e.id_tarjeta  = d.id_tarjeta 
	and e.id_linea  = d.id_linea 
	and e.interno_bus = d.interno_bus 
	and e.hora = d.hora 
	and e.etapa_red_sube = d.etapa_red_sube 
)
delete from etapas e 
using dups d
where e.id < d.id
and e.id_tarjeta  = d.id_tarjeta 
and e.id_linea  = d.id_linea 
and e.interno_bus = d.interno_bus 
and e.hora = d.hora 
and e.etapa_red_sube = d.etapa_red_sube ;



-- SUBTE
-- borrar casos que repiten tarjeta, hora, etapa en SUBTE
with dups as (
	select e.id, e.id_tarjeta,e.id_ramal,e.lat,e.lon, e.hora, e.etapa_red_sube 
	from etapas e,
	(
		select id_tarjeta,id_ramal,lat,lon,hora, etapa_red_sube, count(*)
		from etapas
		where modo = 'SUB'
		group by id_tarjeta,hora,id_ramal,lat,lon,etapa_red_sube
		having count(*) > 1
	) d 
	where e.id_tarjeta  = d.id_tarjeta 
	and e.id_ramal  = d.id_ramal 
	and e.hora = d.hora
	and e.lat = d.lat
	and e.lon = d.lon
	and e.etapa_red_sube = d.etapa_red_sube 
)
delete from etapas e 
using dups d
where e.id < d.id
and e.id_tarjeta  = d.id_tarjeta 
and e.id_ramal  = d.id_ramal 
and e.hora = d.hora 
and e.etapa_red_sube = d.etapa_red_sube ;



-- BORRAR DUPS TREN
with dups as (
	select e.id, e.id_tarjeta,e.id_linea, e.lat,e.lon,e.tipo_trx_tren, e.hora, e.etapa_red_sube 
	from etapas e,
	(
		select id_tarjeta,id_linea,lat,lon,tipo_trx_tren, hora, etapa_red_sube, count(*)
		from etapas
		where modo = 'TRE'
		group by id_tarjeta,id_linea,lat,lon,tipo_trx_tren, hora, etapa_red_sube
		having count(*) > 1
	) d 
	where e.id_tarjeta  = d.id_tarjeta 
	and e.id_linea  = d.id_linea 
	and e.hora = d.hora 
	and e.etapa_red_sube = d.etapa_red_sube
	and e.lat = d.lat
	and e.lon = d.lon
	and e.tipo_trx_tren = d.tipo_trx_tren
)
delete from etapas e 
using dups d
where e.id < d.id
and e.id_tarjeta  = d.id_tarjeta 
and e.id_linea  = d.id_linea 
and e.hora = d.hora 
and e.etapa_red_sube = d.etapa_red_sube
and e.lat = d.lat
and e.lon = d.lon
and e.tipo_trx_tren = d.tipo_trx_tren;


-- Al eliminar estas transacciones pueden quedar tarjeta con una unica transaccion. 
DELETE FROM etapas e
USING (	select id_tarjeta, count(*)
	from etapas 
	group by id_tarjeta
	having count(*) = 1) as u
WHERE e.id_tarjeta = u.id_tarjeta;


--------------------------------------------------------------------------------------------------------------
-- Solucion para doble transaccion misma linea misma hora, sin integracion y sin etapa red sube que ordene secuencialmente 
------------------------------------------------------------------------------------------------------

drop table if exists nuevos_ids_con_etapa_posterior;

-- Crear una tabla para asignar un id nuevo minimizando la distancia con la trx posterior
create table nuevos_ids_con_etapa_posterior as (
	-- Detectar tarjetas misma hora y linea y etapa red sube
	with etapas_duplicadas as ( 
		select id_tarjeta, id_etapa
		from etapas
		where modo in ('SUB', 'COL')
		group by id_tarjeta, id_etapa
		having count(*) >1
	),
	-- Detectar la transaccion posterior a las transacciones duplicadas
	etapas_posteriores as (
		select distinct  on (ed.id_tarjeta, ed.id_etapa) e.id_etapa as id_etapa_posterior, ed.id_tarjeta , ed.id_etapa
		from etapas_duplicadas ed, etapas e
		where ed.id_tarjeta = e.id_tarjeta 
		and ed.id_etapa < e.id_etapa
		ORDER BY ed.id_tarjeta, ed.id_etapa, e.id_etapa
	),
	-- De las transacciones posteriores, usar los checking
	etapas_sin_chkout as (
		select * from etapas where tipo_trx_tren <> 'CHECK OUT' or tipo_trx_tren is null
	),
	-- Calcular distancias 
	distancias_entre_etapas as(
		select ep.*, e1.id as id_trx_etapa, e1.h3 as h3_etapa, e2.id as id_trx_etapa_post, e2.h3 as h3_posterior, h3_distance(e1.h3, e2.h3) as distancia_etapas
		from etapas_posteriores ep
		--inner join etapas e1
		inner join etapas_sin_chkout e1
		on ep.id_tarjeta = e1.id_tarjeta 
		and ep.id_etapa = e1.id_etapa
		inner join etapas_sin_chkout e2
		on ep.id_tarjeta = e2.id_tarjeta 
		and ep.id_etapa_posterior = e2.id_etapa
		order by id_tarjeta, id_etapa, id_etapa_posterior
		),
	-- Calcular nuevo id de etapa
	nuevo_id_etapa as (
		select id_tarjeta, id_etapa, id_etapa_posterior, distancia_etapas,id_trx_etapa,id_trx_etapa_post,	
		row_number () over  (
				PARTITION BY id_tarjeta,id_etapa
				ORDER BY id_etapa, id_etapa_posterior,distancia_etapas asc
				) - 1  as n_fila  
		from distancias_entre_etapas
		),
	-- Puede haber una etapa posterior donde hay doble transaccion 
	chequeo_doble_posterior as ( -- casos como id tarjeta 39005685 que tienen una doble transaccion por delante
		select distinct on (id_tarjeta, id_etapa, id_etapa_posterior,id_trx_etapa) *
		from nuevo_id_etapa
		order by id_tarjeta, id_etapa, id_etapa_posterior,id_trx_etapa,n_fila
		)
	-- Minimizar distancia
	select id_tarjeta, id_etapa, id_etapa_posterior, distancia_etapas, id_trx_etapa, id_trx_etapa_post,
			id_etapa - (row_number () over  (
					PARTITION BY id_tarjeta,id_etapa
					ORDER BY n_fila asc
					) - 1) as nuevo_id_etapa
	from chequeo_doble_posterior
	order by id_tarjeta, id_etapa, nuevo_id_etapa
);

delete from nuevos_ids_con_etapa_posterior
where id_etapa = nuevo_id_etapa;

CREATE INDEX idx_nuevos_ids_con_etapa_posterior
ON nuevos_ids_con_etapa_posterior(id_trx_etapa);

-- Actualizar tabla etapas con los nuevos id posterior
update  etapas e
set id_etapa = np.nuevo_id_etapa
from nuevos_ids_con_etapa_posterior np
where e.id = np.id_trx_etapa;

drop table if exists nuevos_ids_con_etapa_posterior;


-- Crear una tabla para asignar un id nuevo minimizando la distancia con la trx posterior
drop table if exists nuevos_ids_con_etapa_anterior;

create table nuevos_ids_con_etapa_anterior as (
	with etapas_duplicadas as (
		select id_tarjeta, id_etapa
		from etapas
		where modo in ('SUB', 'COL')
		group by id_tarjeta, id_etapa
		having count(*) >1 
	),
	etapas_anteriores as (
		select distinct  on (ed.id_tarjeta, ed.id_etapa) e.id_etapa as id_etapa_anterior, ed.id_tarjeta , ed.id_etapa,
		e.id as id_trx_etapa_pre, e.h3 as h3_anterior
		from etapas_duplicadas ed, etapas e
		where ed.id_tarjeta = e.id_tarjeta 
		and ed.id_etapa > e.id_etapa
		ORDER BY ed.id_tarjeta, ed.id_etapa, e.id_etapa desc,  e.tipo_trx_tren desc -- prioriza el check out en etapa anterior
	),
	distancias_entre_etapas as(
		select ep.*, e1.id as id_trx_etapa, e1.h3 as h3_etapa, 
		h3_distance(e1.h3, ep.h3_anterior) as distancia_etapas
		from etapas_anteriores ep
		inner join etapas e1
		on ep.id_tarjeta = e1.id_tarjeta 
		and ep.id_etapa = e1.id_etapa
		order by id_tarjeta, id_etapa, id_etapa_anterior
		),
	nuevo_id_etapa as (
		select id_tarjeta, id_etapa, id_etapa_anterior, distancia_etapas,id_trx_etapa,id_trx_etapa_pre,	
		id_etapa - (row_number () over  (
				PARTITION BY id_tarjeta,id_etapa
				ORDER BY id_etapa, id_etapa_anterior,distancia_etapas desc
				) - 1)  as nuevo_id_etapa_pre
		from distancias_entre_etapas
		)
	select *
	from nuevo_id_etapa
	order by id_tarjeta, id_etapa, id_etapa_anterior
);

delete from nuevos_ids_con_etapa_anterior
where id_etapa = nuevo_id_etapa_pre;

CREATE INDEX idx_nuevos_ids_con_etapa_anterior
ON nuevos_ids_con_etapa_anterior(id_trx_etapa);

-- Actualizar etapas con los nuevos id anterior
update  etapas e
set id_etapa = np.nuevo_id_etapa_pre
from nuevos_ids_con_etapa_anterior np
where e.id = np.id_trx_etapa;

drop table if exists nuevos_ids_con_etapa_anterior;

-- Las que no tienen anterior ni posterior cambiar al azar
drop table if exists nuevos_ids_azar;

create table nuevos_ids_azar as (
	with etapas_duplicadas as (
		select id_tarjeta, id_etapa
		from etapas
		where modo in ('SUB', 'COL')
		group by id_tarjeta, id_etapa
		having count(*) >1 
	)
	select id, e.id_etapa,
		e.id_etapa - (row_number () over  (
			PARTITION BY e.id_tarjeta,e.id_etapa
			ORDER BY id
			) - 1)  as nuevo_id_etapa
	from etapas_duplicadas ed, etapas e
	where ed.id_tarjeta = e.id_tarjeta 
	and ed.id_etapa = e.id_etapa
	ORDER BY ed.id_tarjeta, ed.id_etapa
	);

delete from nuevos_ids_azar
where id_etapa = nuevo_id_etapa;

CREATE INDEX idx_nuevos_ids_azar
ON nuevos_ids_azar(id);

-- Actualizar etapas con los nuevos id anterior
update  etapas e
set id_etapa = np.nuevo_id_etapa
from nuevos_ids_azar np
where e.id = np.id;

drop table if exists nuevos_ids_azar;
 
------------------------------------------------------------------------------------------------------
-- Agregar atributos geograficos a etapas 
------------------------------------------------------------------------------------------------------
SELECT AddGeometryColumn ('public','etapas','geom',4326,'POINT',2);

update etapas e
SET geom = ST_SetSRID(ST_MakePoint(e.lon, e.lat),4326);

CREATE INDEX etapas_geom_idx
  ON etapas
  USING GIST (geom);

CREATE INDEX etapas_modo
ON etapas(modo);

CREATE INDEX etapas_linea
ON etapas(id_linea);

CREATE INDEX etapas_ramal
ON etapas(id_ramal);

vacuum full etapas;