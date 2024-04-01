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

ejecutar_sql('marketingsql.sql', cur)

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


joblib.dump(movies3,"Salidas\\movies3.joblib") ### para utilizar en segundos modelos

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



