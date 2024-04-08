import numpy as np
import pandas as pd
import sqlite3 as sql
from sklearn.preprocessing import MinMaxScaler
from ipywidgets import interact ## para análisis interactivo
from sklearn import neighbors ### basado en contenido un solo producto consumido
import joblib
from surprise import Reader, Dataset
from surprise.model_selection import cross_validate, GridSearchCV
from surprise import KNNBasic, KNNWithMeans, KNNWithZScore, CoClustering
from surprise.model_selection import train_test_split

conn= sql.connect('C:\marketing\Datos\db_movies')
cur=conn.cursor()

### para ver las tablas que hay en la base de datos
cur.execute("select name from sqlite_master where type='table' ")
cur.fetchall()


########### traer tabla de BD a python ####
movies= pd.read_sql("""select *  from movies""", conn)
ratings = pd.read_sql('select * from ratings', conn)

#### Sistema de recomendación basado en contenido KNN ####

ratings1= pd.read_sql('select * from ratings1', conn)

usuarios=pd.read_sql('select distinct (userId) as user_id from ratings1',conn)

l_movies_r=ratings['movieId'].to_numpy()

ratings1 =ratings1.drop(['index'], axis=1)

movies2 = pd.read_sql(""" select * from movies2""", conn)

movies2=movies2.drop(['level_0','index'],axis=1)

movies3 = pd.read_sql(""" select * from movies3""", conn)

movies3=movies3.drop(['index'],axis=1)

def recomendar(user_id=list(usuarios['user_id'].value_counts().index)):

    ###seleccionar solo los ratings del usuario seleccionado
    ratings2=pd.read_sql('select *from ratings1 where userId=:user',conn, params={'user':user_id})

    ###convertir ratings del usuario a array
    l_pelis_r=ratings2['movieId'].to_numpy()
    movies3[['movieId','title']]=movies2[['movieId','title']]

    ### filtrar peliculas calificadas por el usuario
    pelis_r=movies3[movies3['movieId'].isin(l_pelis_r)]

    ## eliminar columna nombre de movieId
    pelis_r=pelis_r.drop(columns=['movieId','title'])
    pelis_r["indice"]=1 ### para usar group by y que quede en formato pandas tabla de centroide
    ##centroide o perfil del usuario
    centroide=pelis_r.groupby("indice").mean()


    ### filtrar peliculas no vistas
    pelis_nr=movies3[~movies3['movieId'].isin(l_pelis_r)]
    ## eliminbar nombre de movieId
    pelis_nr=pelis_nr.drop(columns=['movieId','title'])

    ### entrenar modelo
    model=neighbors.NearestNeighbors(n_neighbors=11, metric='cosine')
    model.fit(pelis_nr)
    dist, idlist = model.kneighbors(centroide)

    ids=idlist[0] ### queda en un array anidado, para sacarlo
    recomend_b=movies2.loc[ids][['title','movieId']]
    vistas=movies2[movies2['movieId'].isin(l_pelis_r)][['title','movieId']]

    return recomend_b

print(interact(recomendar))

### Sistema de recomendación filtro colaborativo #####

### datos originales en pandas
## knn solo sirve para calificaciones explicitas
ratings3=pd.read_sql('select * from ratings1 where rating>0.0', conn)

####los datos deben ser leidos en un formato espacial para surprise
reader = Reader(rating_scale=(0, 5)) ### la escala de la calificación
###las columnas deben estar en orden estándar: user item rating
data   = Dataset.load_from_df(ratings3[['userId','movieId','rating']], reader)

#####Existen varios modelos
models=[KNNBasic(),KNNWithMeans(),KNNWithZScore(),CoClustering()]
results = {}

###knnBasiscs: calcula el rating ponderando por distancia con usuario/Items
###KnnWith means: en la ponderación se resta la media del rating, y al final se suma la media general
####KnnwithZscores: estandariza el rating restando media y dividiendo por desviación
####CoClustering (Co-Clustering): Agrupa tanto a los usuarios como a los elementos en subgrupos para realizar recomendaciones

#### función para probar varios modelos ##########
model=models[1]
for model in models:

    CV_scores = cross_validate(model, data, measures=["MAE","RMSE"], cv=5, n_jobs=-1)

    result = pd.DataFrame.from_dict(CV_scores).mean(axis=0).\
        rename({'test_mae':'MAE', 'test_rmse': 'RMSE'})
    results[str(model).split("algorithms.")[1].split("object ")[0]] = result


performance_df = pd.DataFrame.from_dict(results).T
performance_df.sort_values(by='RMSE')

###################se escoge el mejor knn  Basic  #########################
param_grid = { 'sim_options' : {'name': ['msd','cosine'], \
                                'min_support': [5], \
                                'user_based': [False, True]}
}


### se afina si es basado en usuario o basado en ítem
gridsearchKNNBasic = GridSearchCV(KNNBasic, param_grid, measures=['rmse'], \
                                    cv=2, n_jobs=2)

gridsearchKNNBasic.fit(data)

gridsearchKNNBasic.best_params["rmse"]
gridsearchKNNBasic.best_score["rmse"]
gs_model=gridsearchKNNBasic.best_estimator['rmse'] ### mejor estimador de gridsearch

################# Entrenar con todos los datos y Realizar predicciones con el modelo afinado

trainset = data.build_full_trainset() ### esta función convierte todos los datos en entrnamiento, las funciones anteriores dividen  en entrenamiento y evaluación
model=gs_model.fit(trainset) ## se reentrena sobre todos los datos posibles (sin dividir)

predset = trainset.build_anti_testset() ### crea una tabla con todos los usuarios y las peliculas que no han leido
#### en la columna de rating pone el promedio de todos los rating, en caso de que no pueda calcularlo para un item-usuario
len(predset)

predictions = gs_model.test(predset) ### función muy pesada, hace las predicciones de rating para todas las peliculas que no hay leido un usuario
### la funcion test recibe un test set constriuido con build_test method, o el que genera crosvalidate

predictions_df = pd.DataFrame(predictions) ### esta tabla se puede llevar a una base donde estarán todas las predicciones
predictions_df.shape
predictions_df.head()
predictions_df['r_ui'].unique() ### promedio de ratings
predictions_df.sort_values(by='est',ascending=False)

##### funcion para recomendar las 10 peliculas con mejores predicciones y llevar base de datos para consultar resto de información
def recomendaciones(userId,n_recomend):

    predictions_userID = predictions_df[predictions_df['uid'] == userId].\
                    sort_values(by="est", ascending = False).head(n_recomend)

    recomendados = predictions_userID[['iid','est']]
    recomendados.to_sql('reco',conn,if_exists="replace")

    recomendados=pd.read_sql('''select a.*, b.title
                            from reco a left join movies2 b
                            on a.iid=b.movieId ''', conn)

    return(recomendados)

##Recomendaciones para el usuario 500
recomendaciones(userId=500,n_recomend=10)