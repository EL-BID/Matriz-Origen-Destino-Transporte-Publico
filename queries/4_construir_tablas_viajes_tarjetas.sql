/*
 Este script crea la tabla de viajes y de tarjetas a partir de las etapas
*/

-- Crear tabla viajes
create table viajes as (
	with primer_y_ultima_etapa as (
		select e.id_tarjeta, e.id_viaje, e.id_etapa, e.hora, e.modo,
			FIRST_VALUE(parada_id_o) over (partition by id_tarjeta, id_viaje order by id_etapa, hora asc
			RANGE BETWEEN
			            UNBOUNDED PRECEDING AND
			            UNBOUNDED FOLLOWING) as parada_id_o,
			LAST_VALUE(parada_id_d) over (partition by id_tarjeta, id_viaje order by id_etapa, hora asc
			RANGE BETWEEN
			            UNBOUNDED PRECEDING AND
			            UNBOUNDED FOLLOWING) as parada_id_d,
			FIRST_VALUE(e.hora) over (partition by id_tarjeta, id_viaje order by id_etapa, hora asc
			RANGE BETWEEN
			            UNBOUNDED PRECEDING AND
			            UNBOUNDED FOLLOWING) as hora_o
		from etapas e
	)
	select id_tarjeta, id_viaje, count(distinct id_etapa) cantidad_etapas,
	sum(CASE when modo = 'SUB'  THEN 1 else 0 end) etapas_subte,
	sum(CASE when modo = 'TRE'  THEN 1 else 0 end) etapas_tren,
	sum(CASE when modo = 'COL'  THEN 1 else 0 end) etapas_colectivo,
	(array_agg(parada_id_o))[1] as parada_id_o,
	(array_agg(parada_id_d))[1] as parada_id_d,
	(array_agg(pu.hora_o))[1] as hora
	from primer_y_ultima_etapa as pu
	group by id_tarjeta, id_viaje
	);

-- clasificar viajes con etapas sin imputar destino correctamente
alter table viajes
add column etapas_incompletas bool;


UPDATE viajes v
SET etapas_incompletas = true
FROM (
	select distinct id_tarjeta, id_viaje
	from etapas
	where parada_id_d is null) p
WHERE v.id_tarjeta = p.id_tarjeta
and v.id_viaje = p.id_viaje;


UPDATE viajes v
SET etapas_incompletas = false
where etapas_incompletas is not true;


-- Crear tabla tarjetas
create table tarjetas as (
	select distinct id_tarjeta, FIRST_VALUE(parada_id_o) over (partition by id_tarjeta order by id_viaje asc
			RANGE BETWEEN
			            UNBOUNDED PRECEDING AND
			            UNBOUNDED FOLLOWING) as parada_hogar
from viajes);

create table tabla_sexo_tarjeta as
with sexo_tarjetas as (
	select distinct id_tarjeta,sexo 
	from etapas
	where sexo <> ''
	)
select *
from sexo_tarjetas
where id_tarjeta not in (
	-- tarjetas con mas de un sexo
	select id_tarjeta
	from sexo_tarjetas
	group by id_tarjeta
	having count(*) > 1
);

alter table tarjetas
add column sexo text;

UPDATE tarjetas t
SET sexo = st.sexo
FROM tabla_sexo_tarjeta st
WHERE t.id_tarjeta = st.id_tarjeta;

drop table tabla_sexo_tarjeta; 

alter table tarjetas
add column viajes_con_etapas_incompletas bool;


UPDATE tarjetas t
SET viajes_con_etapas_incompletas = true
FROM (
	select distinct id_tarjeta
	from viajes
	where etapas_incompletas = true) v
WHERE t.id_tarjeta = v.id_tarjeta;

UPDATE tarjetas t
SET viajes_con_etapas_incompletas = false
where viajes_con_etapas_incompletas is not true;

