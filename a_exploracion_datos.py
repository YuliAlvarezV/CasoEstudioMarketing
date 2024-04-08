####EXPLORACION DE DATOS####
import numpy as np
import pandas as pd
import sqlite3 as sql
import plotly.graph_objs as go ### para gráficos
import plotly.express as px
import sys ## saber ruta de la que carga paquetes
from mlxtend.preprocessing import TransactionEncoder
from sklearn.preprocessing import MinMaxScaler
import joblib

###### para ejecutar sql y conectarse a bd ###
conn=sql.connect('C:\\marketing\\Datos\\db_movies') ### crear cuando no existe el nombre de cd  y para conectarse cuando sí existe.
cur=conn.cursor() ###para funciones que ejecutan sql en base de datos


###Ruta directorio qué tiene paquetes
sys.path
sys.path.append('C:\\marketing\\CasoEstudioMarketing') ## este comanda agrega una ruta

## Traer el archivo funciones a este archivo

import funciones as func

#### para consultar datos ######## con cur

cur.execute("select * from movies")
cur.fetchall()

#### Consultas de las tablas

### para ver las tablas que hay en la base de datos
cur.execute("select name from sqlite_master where type='table' ")
cur.fetchall()
########### traer tabla de BD a python ####
movies= pd.read_sql("""select *  from movies""", conn)
ratings = pd.read_sql('select * from ratings', conn)

### Identificar cada tabla y su información ####
### verificar duplicados y nulos

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
calificacion=pd.read_sql(""" select
                            "rating" as calificación,
                            count(*) as cantidad
                            from ratings
                            group by "rating"
                            order by cantidad desc""", conn)
calificacion ##Verificar tabla de distribución calificaciones

##Grafico de las distribuciones
data  = go.Bar( x= calificacion.calificación,y=calificacion.cantidad, text=calificacion.cantidad, textposition="outside")
Layout=go.Layout(title="Conteo de calificaciones",xaxis={'title': 'Calificación', 'tickvals': calificacion.calificación},yaxis={'title':'Conteo'})
fig=go.Figure(data,Layout)
# Mostrar la figura
fig.show()

### calcular cada usuario cuantas peliculas calificó
rating_usersconteo=pd.read_sql(''' select "userId" as user_id,
                            count(*) as calificación
                            from ratings
                            group by "user_id"
                            order by calificación asc
                            ''',conn )
rating_usersconteo##Verificar tabla
fig  = px.histogram(rating_usersconteo, x= 'calificación', title= 'Número de calificaciones por usario')
fig.show()

## Tabla de suarios que hayan calificado más de 15 peliculas y menos de 400 para obtener una mejor distribución
rating_users2=pd.read_sql(''' select "userId" as userId,
                            count(*) as calificación
                            from ratings
                            group by "userId"
                            having calificación >=15 and calificación <=400
                            order by calificación asc
                            ''',conn )
rating_users2 ##Verificar tabla

fig  = px.histogram(rating_users2, x= 'calificación', title= 'Número de calificaciones por usario mejorando la distribución')
fig.show()

## Tabla de acuerdo a la calificación de cada pelicula, top 5 de las más calificadas
rating_movies=pd.read_sql(''' select movieId ,
                            count(*) as calificación
                            from ratings
                            group by "movieId"
                            order by calificación desc limit 5
                            ''',conn )
rating_movies
rating_movies = rating_movies.astype({'movieId': 'str'}) ## Se transforma a str para poder ver la distribución en gráfica
rating_movies.info() ##Verificar el cambio
data  = go.Bar( x= rating_movies.movieId,y=rating_movies.calificación, text=rating_movies.movieId, textposition="outside")
Layout=go.Layout(title="Total de calificaciones por película",xaxis={'title': 'movieId'},yaxis={'title':'Conteo'})
figura=go.Figure(data,Layout)
figura.show()

func.ejecutar_sql('marketingsql.sql', cur)

cur.execute("select name from sqlite_master where type='table' ")
cur.fetchall()

##Se dividen los generos 
movies=pd.read_sql("""select * from movies""", conn)
genres=movies['genres'].str.split('|')
te = TransactionEncoder()
genres = te.fit_transform(genres)
genres = pd.DataFrame(genres, columns = te.columns_)

genres= genres.drop(['(no genres listed)'], axis=1) # Esta columna no da información

genres2= genres.copy() ##Se copia la tabla para realizar modificaciones

genres2 = genres2.replace({True: 1, False: 0}) ##### Se cambia True por 1 y False por 0

genres2 = genres2.astype(int) ## Se cambia el tipo a entero

genres2.info()

##Insertar la columna de movieId y el title
genres2.insert(0, 'movieId', movies['movieId'])
genres2.insert(1, 'title', movies['title'])

genres2.to_sql('genres2',conn, if_exists='replace' ) ##enviar genres 2 a base de datos sql

cur.execute("ALTER TABLE genres2 ADD COLUMN fecha INTEGER") #Se agrega la columna fecha

##Extraer año del titulo (estreno de la pelicula)
cur.execute("""
    UPDATE genres2
    SET fecha = CAST(SUBSTR(title, -5, 4) AS INTEGER)
""")

movies2 = pd.read_sql(""" select * from genres2""", conn)

## Escalar fecha
sc=MinMaxScaler()
movies2[["year_sc"]]=sc.fit_transform(movies2[['fecha']])

movies2.to_sql('movies2',conn, if_exists='replace' )

movies3= movies2.drop(columns=['title','index','fecha', 'movieId'])

movies3.to_sql('movies3',conn, if_exists='replace' )

### joblib.dump(movies3,"CasoEstudioMarketing\\movies3.joblib") ### para utilizar en segundos modelos

##Utilizar tabla de rating_final para los modelos
rating_final= pd.read_sql('select * from rating_final', conn)

rating_final['fecha']= pd.read_sql("""SELECT fecha FROM fecha_nueva""", conn) ###Convertir columna timestamp a formato fecha

ratings1= rating_final.copy() ##Se hace una copia de rating_final

##Se elimina el timestamp de la tabla de ratings, para no afectar los sistemas de recomendación
ratings1= ratings1.drop(['timestamp'],axis=1)

ratings1.to_sql('ratings1',conn, if_exists='replace' )

##Crear tabla para la realización de modelos
ratingtitle= pd.read_sql("""select b.*,a.title
            from ratings1 b left join movies a on b.movieId=a.movieId
            """, conn)

ratingtitle =ratingtitle.drop(['index'], axis=1)#Se elimina la columna index

ratingtitle.to_sql('ratingtitle',conn, if_exists='replace' ) #enviar ratingtittle a sql