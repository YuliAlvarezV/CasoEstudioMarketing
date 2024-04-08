import numpy as np
import pandas as pd
import sqlite3 as sql
import openpyxl
from sklearn.preprocessing import MinMaxScaler
####Paquete para sistema basado en contenido ####
from sklearn.preprocessing import MinMaxScaler
from sklearn import neighbors
import sys ## saber ruta de la que carga paquetes

###Ruta directorio qué tiene paquetes
sys.path
sys.path.append('C:\\marketing\\CasoEstudioMarketing') ## este comanda agrega una ruta

## Traer el archivo funciones a este archivo

import funciones as func

def preprocesar():

    #### conectar_base_de_Datos#################
    conn=sql.connect('C:\\marketing\\Datos\\db_movies') 
    cur=conn.cursor()
    

    ######## convertir datos crudos a bases filtradas por usuarios que tengan cierto número de calificaciones
    func.ejecutar_sql('C:\marketing\CasoEstudioMarketing\marketingsql.sql', cur)

    ##### llevar datos que cambian constantemente a python ######
    movies=pd.read_sql('select * from movies2', conn )
    ratings=pd.read_sql('select * from ratings1', conn)
    usuarios=pd.read_sql('select distinct (userId) as user_id from ratings1',conn)


    ## eliminar filas que no se van a utilizar ###
    movies6=movies.drop(columns=['movieId','level_0','fecha','title','index'])

    return movies6, movies, conn, cur

###Función para entrenar modelo por cada usuario ###

###Basado en contenido todo lo visto por el usuario Knn###

def recomendar(user_id):
    
    movies6, movies, conn, cur= preprocesar()
    
    ratings2=pd.read_sql('select *from ratings1 where userId=:user',conn, params={'user':user_id})
    l_pelis_r=ratings2['movieId'].to_numpy()
    movies6[['movieId','title']]=movies[['movieId','title']]
    pelis_r=movies6[movies6['movieId'].isin(l_pelis_r)]
    pelis_r=pelis_r.drop(columns=['movieId','title'])
    pelis_r["indice"]=1 ### para usar group by y que quede en formato pandas tabla de centroide
    centroide=pelis_r.groupby("indice").mean()
    
    
    pelis_nr=movies6[~movies6['movieId'].isin(l_pelis_r)]
    pelis_nr=pelis_nr.drop(columns=['movieId','title'])
    model=neighbors.NearestNeighbors(n_neighbors=11, metric='cosine')
    model.fit(pelis_nr)
    dist, idlist = model.kneighbors(centroide)
    
    ids=idlist[0]
    recomend_b=movies.loc[ids][['title','movieId']]
    
    return recomend_b

recomendar(500)