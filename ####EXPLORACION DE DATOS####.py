####EXPLORACION DE DATOS####
import numpy as np
import pandas as pd
import sqlite3 as sql
import plotly.graph_objs as go ### para gráficos
import plotly.express as px
###import a_funciones as fn

###pip install  pysqlite3

###### para ejecutar sql y conectarse a bd ###

## crear copia de db_books datos originales, nombrarla books2 y procesar books2

conn=sql.connect('C:\\Users\\yulia\\Desktop\\EntregaMarketing\\db_movies') ### crear cuando no existe el nombre de cd  y para conectarse cuando sí existe.
cur=conn.cursor() ###para funciones que ejecutan sql en base de datos

#### para consultar datos ######## con cur

cur.execute("select * from movies")
cur.fetchall()

##### consultar trayendo para pandas ###
df_movies=pd.read_sql("select * from movies", conn)


#### para ejecutar algunas consultas

### para ver las tablas que hay en la base de datos
cur.execute("select name from sqlite_master where type='table' ")
cur.fetchall()
########### traer tabla de BD a python ####
movies= pd.read_sql("""select *  from movies""", conn)
ratings = pd.read_sql('select * from ratings', conn)

#####Exploración inicial #####

### Identificar campos de cruce y verificar que estén en mismo formato ####
### verificar duplicados

movies.info()
movies.head()
movies.duplicated().sum()

ratings.info()
ratings.head()
ratings.duplicated().sum()

###No se ven datos duplicados ni nulos

##################################################################3
##### Descripción base de ratings
###calcular la distribución de calificaciones
cali=pd.read_sql(""" select
                          "rating" as calificación,
                          count(*) as cantidad
                          from ratings
                          group by "rating"
                          order by cantidad desc""", conn)
cali ##Verificar tabla de distribución calificaciones

##Grafico de las distribuciones
data  = go.Bar( x= cali.calificación,y=cali.cantidad, text=cali.cantidad, textposition="outside")
Layout=go.Layout(title="Conteo de calificaciones",xaxis={'title': 'Calificación', 'tickvals': cali.calificación},yaxis={'title':'Conteo'})
fig=go.Figure(data,Layout)
# Mostrar la figura
fig.show()

### calcular cada usuario cuantas peliculas calificó
rating_users=pd.read_sql(''' select "userId" as user_id,
                         count(*) as calificación
                         from ratings
                         group by "user_id"
                         order by calificación asc
                         ''',conn )
rating_users##Verificar tabla
fig  = px.histogram(rating_users, x= 'calificación', title= 'Hist frecuencia de número de calificaciones por usario')
fig.show()

## Crear tabla con usuarios que hayan calificado más de 20 peliculas y menos de 600 para obtener una mejor distribución
rating_users2=pd.read_sql(''' select "userId" as userId,
                         count(*) as calificación
                         from ratings
                         group by "userId"
                         having calificación >=20 and calificación <=600
                         order by calificación asc
                         ''',conn )
rating_users2 ##Verificar tabla

fig  = px.histogram(rating_users2, x= 'calificación', title= 'Hist frecuencia de número de calificaciones por usario')
fig.show()

## Crear tabla de acuerdo a la calificación de cada pelicula, para observar el top 10 de las más calificadas
rating_movies=pd.read_sql(''' select movieId ,
                         count(*) as calificación
                         from ratings
                         group by "movieId"
                         order by calificación desc limit 10
                         ''',conn )
rating_movies
rating_movies = rating_movies.astype({'movieId': 'str'}) ## Se transdorma a str para poder ver la distribución en gráfica
rating_movies.info() ##Verificar el cambio
data  = go.Bar( x= rating_movies.movieId,y=rating_movies.calificación, text=rating_movies.movieId, textposition="outside")
Layout=go.Layout(title="Conteo de calificaciones",xaxis={'title': 'movieId'},yaxis={'title':'Conteo'})
go.Figure(data,Layout)