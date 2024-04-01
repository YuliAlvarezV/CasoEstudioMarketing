--Seleccionar usuarios que hayan calificado
drop table if exists rating_user2;
create table rating_user2 AS
select userId as userId,
count(*) as calificación
from ratings
group by userId
having calificación >=20 and calificación <=600
order by calificación asc;

--Unir usuarios final con ratings original
drop table if exists rating_final;
create table rating_final AS
select a.*
from ratings a 
inner join rating_user2 b
on a.userId =b.userId;

-- Crear tabla con formato fecha
drop table if exists fecha_nueva;
create table fecha_nueva AS
SELECT timestamp,
strftime('%Y', timestamp, 'unixepoch') AS fecha FROM rating_final;