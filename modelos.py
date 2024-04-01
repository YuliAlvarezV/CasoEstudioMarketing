import numpy as np
import pandas as pd
import sqlite3 as sql
from sklearn.preprocessing import MinMaxScaler
from ipywidgets import interact ## para análisis interactivo
from sklearn import neighbors ### basado en contenido un solo producto consumido
import joblib

##### conectarse a BD #######
conn= sql.connect('C:\\marketing\\Datos\\db_movies')
cur=conn.cursor()


### para ver las tablas que hay en la base de datos
cur.execute("select name from sqlite_master where type='table' ")
cur.fetchall()


########### traer tabla de BD a python ####
movies= pd.read_sql("""select *  from movies""", conn)
ratings = pd.read_sql('select * from ratings', conn)

###### Sistemas basados en popularidad ##############

#### mejores peliculas calificadas que tengan una calificación diferente de 0.0
pd.read_sql("""select title,
            avg(rating) as avg_rat,
            count(*) as watch_num
            from ratingtitle
            where rating<>0.0
            group by title
            having watch_num >=15 and watch_num <=100
            order by avg_rat desc
            limit 10
            """, conn)


#### las peliculas mas vistas con promedio de los que calificaron ###
pd.read_sql("""select title,
            avg(iif(rating = 0, Null, rating)) as avg_cal,
            count(*) as watch_num
            from ratingtitle
            group by title
            order by watch_num desc
            limit 10
            """, conn)


#### las mejores peliculas calificadas según su fecha de visualizacion ###
pd.read_sql("""select fecha, title,
            avg(iif(rating = 0.0, Null, rating)) as avg_cal,
            count(iif(rating = 0.0, Null, rating)) as watch_num
            from ratingtitle
            group by title
            having watch_num >=15 and watch_num <=100
            order by avg_cal desc
            limit 15
            """, conn)

##### Sistema de recomendacion basado en contenido de un producto #########################33

movies2 = pd.read_sql(""" select * from movies2""", conn)

movies2=movies2.drop(['level_0','index'],axis=1)

movies3 = pd.read_sql(""" select * from movies3""", conn)

movies3=movies3.drop(['index'],axis=1)

### Ejemplo manual para verificar que corra bien
movie='Cutthroat Island (1995)'
ind_movie=movies2[movies2['title']==movie].index.values.astype(int)[0]
similar_movies=movies3.corrwith(movies3.iloc[ind_movie,:],axis=1)
similar_movies=similar_movies.sort_values(ascending=False)
top_similar_movies=similar_movies.to_frame(name="correlación").iloc[0:11,] ### el 11 es número de peliculas recomendadas
top_similar_movies['title']=movies2["title"]


##Para imprimir las 10 peliculas más similares a la pelicula seleccionada
print(top_similar_movies)

#### Función de peliculas recomendadas

def movierecomendacion(movie = list(movies2['title'])):

    ind_movie=movies2[movies2['title']==movie].index.values.astype(int)[0]   #### obtener indice de la pelicula seleccionada de lista original
    similar_movies = movies3.corrwith(movies3.iloc[ind_movie,:],axis=1) ## correlación entre pelicula seleccionada y todas las otras
    similar_movies = similar_movies.sort_values(ascending=False) #### ordenar correlaciones
    top_similar_movies=similar_movies.to_frame(name="correlación").iloc[0:11,] ### el 11 es número de peliculas recomendadas
    top_similar_movies['title']=movies2["title"] ### agregaro los nombres (como tiene mismo indice no se debe cruzar)

    return top_similar_movies


print(interact(movierecomendacion))

######### Sistema de recomendación basado en contenido KNN un solo producto #########

model = neighbors.NearestNeighbors(n_neighbors=11, metric='cosine')
model.fit(movies3)
dist, idlist = model.kneighbors(movies3)

distancias=pd.DataFrame(dist) ## devuelve un ranking de la distancias más cercanas para cada fila(movies)
id_list=pd.DataFrame(idlist) ## para saber esas distancias a que item corresponde


####ejemplo para una pelicula para verificación
movie_list_name = []
movie_name='Emma (1996)'
movie_id = movies2[movies2['title'] == movie_name].index ### extraer el indice de la pelicula
movie_id = movie_id[0] ## si encuentra varios solo guarde uno
for newid in idlist[movie_id]:
        movie_list_name.append(movies.loc[newid].title) ### agrega el nombre de cada una de los id recomendados

movie_list_name

def MovieRecommender(movie_name = list(movies2['title'].value_counts().index)):
    movie_list_name = []
    movie_id = movies2[movies2['title'] == movie_name].index ### extraer el indice de la pelicula
    movie_id = movie_id[0] ## si encuentra varios solo guarde uno
    for newid in idlist[movie_id]:
        movie_list_name.append(movies.loc[newid].title) ### agrega el nombre de cada una de los id recomendados

    return(movie_list_name)


print(interact(MovieRecommender))