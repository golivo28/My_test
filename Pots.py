#Importar paquetes requeridos

import pandas as pd
import pyodbc
import numpy as np
import os

#------------------------------

#Crear conexión con base de datos

conn = pyodbc.connect("""DRIVER={SQL Server};
                         SERVER=proyectos.tekus.co;
                         DATABASE=DataTest;
                         UID=datatest;
                         PWD=9cUQ*48AAX8Q;
                         """)
cursor = conn.cursor()

#------------------------------

#Leer archivos csv en directorio donde se encuentran ubicados.

route = r'Documents\Prueba Tekus' #---> Este directorio es donde reposan las carpetas nombradas con el código de cada olla
df = pd.DataFrame()

for i in os.listdir(route):       #---> Iterar por carpetas hasta encontrar los archivos csv
    b = os.path.join(route,i)
    for j in os.listdir(b):
        c = os.path.join(b,j)
        for k in os.listdir(c):
            d = os.path.join(c,k)
            for l in os.listdir(d):
                e = os.path.join(d,l)
                for m in os.listdir(e):
                    f = os.path.join(e,m)
                    df = pd.concat([df,pd.read_csv(f)]) #---> Leer archivos csv y concatenarlos en un solo dataframe
                    
#----------------------------

#Limpiar datos

df.drop(columns = ['ArkboxInteractions','StandardInteractions','TenantId','SessionId','Id','DateInTicks','TTL'],inplace = True) #---> Borrar
                                                                                                                                #columnas
                                                                                                                                #que no
                                                                                                                                #serán
                                                                                                                                #tenidas en
                                                                                                                                #cuenta.
df = df.reset_index(drop = True)  #---> Resetear index para facilitar manipulación

df['DatetimeFmt'] = pd.to_datetime(df['Date'].apply(lambda x: x[0:24])) #---> Transformar fechas en str a datetime
df['DayofWeek'] = df['DatetimeFmt'].dt.day_name()                       #---> Extraer día de la fecha convertida a datetime
df['Time'] = df['DatetimeFmt'].dt.time                                  #---> Extraer hora de la fecha convertida a datetime

for i in df['Key'].unique():      #---> Iterar por los códigos para calcular diferencias entre registros consecutivos que le pertenezcan.
    df.loc[df[df['Key']==i].index,'Differences'] = df[df['Key']==i]['DatetimeFmt'].diff(periods = 1)\
    .dt.seconds                   #---> Convertir diferencia en segundos
    
df['Differences'] = df['Differences'].astype('float64',errors='raise') #---> Convertir diferencias en float64

for k,i in enumerate(df['Differences']): #---> Reposicionar diferencias en filas correspondientes
    if k+1 < len(df):
        df.loc[k,'Differences'] = df['Differences'][k+1]
        df.loc[k+1,'Differences'] = np.nan
    else:
        break
        
for i in df['Key'].unique():            #---> Asignar media de diferencias por olla a filas vacías
    a = df[df['Key'] == i]['Differences'].mean()
    df.loc[df[(df['Key'] == i) & (df['Differences'].isnull())].index, 'Differences'] = a

df['AnomalousDuration'] = df['Duration'] > df['Differences']                  #---> etiquetar valores de duración anómalos
df['AnomalousMovementDuration'] = df['MovementDuration'] > df['Differences']  #---> etiquetar valores de duración de movimiento anómalos

df = df[~((df['AnomalousDuration'] == True) | (df['AnomalousMovementDuration'] == True))] #---> excluir valores anómalos

df = df[['Duration','MovementDuration','MovementInteractions','HardwareInteractions','Key','DatetimeFmt','DayofWeek','Time']]

df.rename(columns = {'DatetimeFmt':'Date',
                     'Key':'PotKey'},inplace = True)

#---------------------------

#Cargar datos

cursor.execute("""DROP TABLE IF EXISTS pots_data""")

cursor.execute("""CREATE TABLE pots_data (Duration INTEGER, 
                                          MovementDuration INTEGER,
                                          MovementInteractions INTEGER,
                                          HardwareInteractions INTEGER,
                                          PotKey TEXT,
                                          Date TEXT,
                                          DayofWeek TEXT,
                                          Time TEXT)""")


for i in df.itertuples():
    cursor.execute("""INSERT INTO pots_data (Duration,
                                             MovementDuration,
                                             MovementInteractions,
                                             HardwareInteractions,
                                             PotKey,
                                             Date.
                                             DayofWeek,
                                             Time)""",
                  i.Duration,
                  i.MovementDuration,
                  i.MovementInteractions,
                  i.HardwareInteractions,
                  i.PotKey,
                  i.Date,
                  i.DayofWeek,
                  i.Time)
conn.commit()
conn.close()
