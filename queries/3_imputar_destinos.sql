/*
Este script imputa a cada etapa una parada de origen y de destino a las etapas. 

Cuando se calculan distancias se usan las de h3 que calcula el camino mas corto siguiendo celdas adyacentes.
Las celdas utilizadas en este proceso tienen resolucion 10, es decir que el largo del hexagono tiene 66 m. 
Entre los centros de dos hexagonos adyacentes hay 132 m de distancia. 

Distancias de 15 hex significa ~ 2000 metros
Distancias de 16 hex significa ~ 2100 metros
Distancias de 17 hex significa ~ 2200 metros 

Selecciona para cada etapa una estacion de origen tomando aquella con la distancia minima
siempre dentro de un area de influencia minima (buffer) de la estacion
Se usa ramal para FFCC y Subte, linea para buses. Porque, por ej., si la etapa dice usar la linea C (considerada como ramal),
hay que comparar la distancia contra ese ramal, y no todas las estaciones del subte (considerado como linea)
*/

create table distancia_minima_parada_o as (
	with buses as (
		select distinct on (e_id) * 
		from (
			-- selecciona la estaciones cercanas
			select 		
				e.id e_id,
				e.id_linea e_linea,
				e.id_ramal e_ramal,
				e.modo e_modo,
				e.h3 h3_etapas,
				p.id p_id,
				p.h3 h3_paradas,
				p.id_linea p_linea,
				p.id_ramal p_ramal,
				h3_distance(e.h3,p.h3) distancia
			from (select * from etapas where modo = 'COL') as e 
			inner join paradas as p
			on e.id_linea = p.id_linea
			and ST_Intersects(e.geom, p.geom) 
		) d
		order by e_id, distancia),
	trenes as (
		select distinct on (e_id) * 
		from (
			-- selecciona la estaciones cercanas
			select 		
				e.id e_id,
				e.id_linea e_linea,
				e.id_ramal e_ramal,
				e.modo e_modo,
				e.h3 h3_etapas,
				p.id p_id,
				p.h3 h3_paradas,
				p.id_linea p_linea,
				p.id_ramal p_ramal,
				h3_distance(e.h3,p.h3) distancia
			from (select * from etapas where modo = 'TRE') as e 
			inner join paradas as p
			on e.id_ramal = p.id_ramal
			and ST_Intersects(e.geom, p.geom) 
		) d
		order by e_id, distancia),
	subtes as (
		select distinct on (e_id) * 
		from (
			-- selecciona la estaciones cercanas
			select 		
				e.id e_id,
				e.id_linea e_linea,
				e.id_ramal e_ramal,
				e.modo e_modo,
				e.h3 h3_etapas,
				p.id p_id,
				p.h3 h3_paradas,
				p.id_linea p_linea,
				p.id_ramal p_ramal,
				h3_distance(e.h3,p.h3) distancia
			from (select * from etapas where modo = 'SUB') as e 
			inner join paradas as p
			on e.id_ramal = p.id_ramal
			and ST_Intersects(e.geom, p.geom) 
		) d
		order by e_id, distancia) 
		select * from subtes
		union 
		select * from trenes
		union 
		select * from buses
);


--Agregar a la tabla etapas una columna para el id de parada de origen 
alter table etapas
ADD COLUMN parada_id_o numeric;

/*Imputar la parada de origen a la tabla etapa , siempre que la distancia a la parada de origen
no sea mayor que a 800 metros (6 hexagonos h3 a resolucion 10)
*/
delete from distancia_minima_parada_o where distancia > 6;

CREATE INDEX etapas_ids_distancia_minima_parada_o_e_id
ON distancia_minima_parada_o(e_id);

update etapas e
set parada_id_o = d.p_id 
from distancia_minima_parada_o d
where e.id = d.e_id;


-- Elminar tabla insumo
drop table distancia_minima_parada_o;

-- Borrar transacciones con distancias muy largas a su parada de origen
DELETE FROM etapas e where parada_id_o is null;

-- Borrar tarjetas simple transaccion
DELETE FROM etapas e
USING (	select id_tarjeta, count(*)
	from etapas 
	group by id_tarjeta
	having count(*) = 1) as u
WHERE e.id_tarjeta = u.id_tarjeta;


--------------------------------------------------------------------------------------------------------------------
-- IMPUTAR DESTINOS
--------------------------------------------------------------------------------------------------------------------
/*
Crear una tabla con los posibles destinos de cada etapa, empezando por asignar como pontencial destino
la siguiente transaccion de esa misma tarjeta. Al ordenar por tipo de trx tren, el checkout queda 
como la siguiente transaccion y por ende, el destino potencial
*/

-- Se crea una tabla de potenciales destinos, con la siguiente transaccion como destino propuesto
create table destinos as (
select id, id_tarjeta, id_etapa, id_viaje,id_linea,hora, tipo_trx_tren,
	LEAD(id,1) OVER (
		PARTITION BY id_tarjeta
		ORDER BY id_viaje,id_etapa,tipo_trx_tren 
	) id_lead,
	LEAD(geom,1) OVER (
		PARTITION BY id_tarjeta
		ORDER BY id_viaje,id_etapa,tipo_trx_tren 
	) geom,
	LEAD(h3,1) OVER (
		PARTITION BY id_tarjeta
		ORDER BY id_viaje,id_etapa,tipo_trx_tren 
	) h3_lead,
	row_number () over  (
		PARTITION BY id_tarjeta
		ORDER BY id_viaje,id_etapa,tipo_trx_tren
	) fila
from etapas e
ORDER BY id_viaje,id_etapa,tipo_trx_tren);


CREATE INDEX idx_destinos_id_lead
ON destinos(id_lead);

CREATE INDEX idx_destinos_id
ON destinos(id);

CREATE INDEX idx_destinos_id_tarjeta
ON destinos(id_tarjeta);

-- La última transaccion del dia no tiene siguiente transaccion, por lo que se usa la primera del dia
update destinos d
set id_lead = p.primera
from (
	select distinct on (id_tarjeta) id_tarjeta, id as primera
	from destinos 
	order by id_tarjeta, fila
) p
where id_lead is null
and d.id_tarjeta = p.id_tarjeta;


-- Eliminar check outs a los que no nos interesa imputar destinos 
delete from destinos where tipo_trx_tren = 'CHECK OUT'; 

/*
Agregar a la tabla de potenciales destinos los atributos geograficos del destino
**/  
update destinos d
set 
	geom = e.geom,
	h3_lead = e.h3
from etapas e
where d.id_lead = e.id
and d.geom is null
and d.h3_lead is null
;

/*
Se crea una tabla con la parada mas cercana al potencial destino 
*/

CREATE INDEX destinos_geom_idx
  ON destinos
  USING GIST (geom);

-- Para todas las paradas posibles del destino, elegir la que minimice la distancia 
create table distancia_minima_parada_d as (
	select distinct on (id_tarjeta,id_viaje,id_etapa) * 
	from (
		select d.*,p.id as p_id, p.h3 as h3_parada, h3_distance(d.h3_lead,p.h3) distancia
		from destinos d
		inner join paradas as p
		on d.id_linea = p.id_linea
		and ST_Intersects(d.geom, p.geom)
		) t
	order by id_tarjeta,id_viaje,id_etapa,tipo_trx_tren,distancia
);

-- Elminar como pontenciales paradas de destino aquellas con mas de 2200 metros con respecto a la siguiente transaccion
delete from distancia_minima_parada_d where distancia > 17;

CREATE INDEX idx_distancia_minima_parada_d_id
ON distancia_minima_parada_d(id);

alter table etapas
ADD COLUMN parada_id_d numeric;

vacuum full etapas;

-- Agregar parada destino a la tabla etapas
update etapas e
set parada_id_d = d.p_id 
from distancia_minima_parada_d d
where e.id = d.id;

-- Se eliminan checkouts que no son etapas propiamente dichas, sino destinos de etapas
delete from etapas where tipo_trx_tren = 'CHECK OUT';


select count(*) from etapas where parada_id_d is not null;

-- eliminar tablas insumo
DROP TABLE destinos;
DROP TABLE distancia_minima_parada_d;


