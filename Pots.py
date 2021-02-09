#Importar paquetes requeridos

import pandas as pd
import pyodbc
import numpy as np
import os

#***********************************************CONECTAR CON BASE DE DATOS EN SQL SERVER******************************************#
                                                                                                                                  #
my_conn = pyodbc.connect("DRIVER={SQL Server};SERVER=proyectos.tekus.co;DATABASE=DataTest;UID=datatest;PWD=9cUQ*48AAX8Q;")        # 
cursor = my_conn.cursor()                                                                                                         #
                                                                                                                                  #
df_pots = pd.read_sql_query('SELECT * FROM Pots',my_conn)       #--> será usado más adelante                                      #
df_cities = pd.read_sql_query('SELECT * FROM Cities',my_conn)   #--> será usado más adelante                                      #
                                                                                                                                  #
#***********************************************FIN CONECTAR CON BASE DE DATOS EN SQL SERVER**************************************#

#***********************************************LEER CSV's************************************************************************#
#                                                                                                                                 #
route = r'Documents\Prueba Tekus'    #---> Directorio de carpeta que contiene datos de las ollas                                  #
df = pd.DataFrame()                                                                                                               #
                                                                                                                                  #
for i in os.listdir(route):                                                                                                       #
    b = os.path.join(route,i)                                                                                                     #
    for j in os.listdir(b):                                                                                                       #
        c = os.path.join(b,j)                                                                                                     #
        for k in os.listdir(c):                                                                                                   #
            d = os.path.join(c,k)                                                                                                 #
            for l in os.listdir(d):                                                                                               #
                e = os.path.join(d,l)                                                                                             #
                for m in os.listdir(e):                                                                                           #
                    f = os.path.join(e,m)                                                                                         #
                    df = pd.concat([df,pd.read_csv(f)])                                                                           #
                                                                                                                                  #
#***********************************************FIN LEER CSV's********************************************************************#

#***********************************************LIMPIAR DATOS*********************************************************************#
#                                                                                                                                 *
#---Excluir columnas no usadas----------------------------------------------------------------------------------------------------#
df.drop(columns = ['HardwareInteractions','StandardInteractions','TenantId','SessionId','Id','DateInTicks','TTL'],inplace = True) #
df = df.reset_index(drop = True)                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Detectar y excluir duraciones que excedan diferencia entre registros consecurivos---------------------------------------------#
df['DatetimeFmt'] = pd.to_datetime(df['Date'].apply(lambda x: x[0:24]))                                                           #
df['WeekDay'] = df['DatetimeFmt'].dt.day_name()                                                                                   #
df['Hour'] = df['DatetimeFmt'].dt.hour                                                                                            #
                                                                                                                                  #
#-----                                                                                                                            #
for i in df['Key'].unique():                                                                                                      #
    df.loc[df[df['Key']==i].index,'Differences'] = df[df['Key']==i]['DatetimeFmt'].diff(periods = 1).dt.seconds                   #
                                                                                                                                  #
df['Differences'] = df['Differences'].astype('float64',errors='raise')                                                            # 
                                                                                                                                  #
#-----                                                                                                                            #
for k,i in enumerate(df['Differences']):                                                                                          #
    if k+1 < len(df):                                                                                                             #
        df.loc[k,'Differences'] = df['Differences'][k+1]                                                                          #
        df.loc[k+1,'Differences'] = np.nan                                                                                        #
    else:                                                                                                                         #
        break                                                                                                                     #
                                                                                                                                  #
#-----                                                                                                                            #
for i in df['Key'].unique():                                                                                                      #
    a = df[df['Key'] == i]['Differences'].mean()                                                                                  #
    df.loc[df[(df['Key'] == i) & (df['Differences'].isnull())].index, 'Differences'] = a                                          #
                                                                                                                                  # 
#-----                                                                                                                            # 
df['AnomalousDuration'] = df['Duration'] > df['Differences']                                                                      #
df['AnomalousMovementDuration'] = df['MovementDuration'] > df['Differences']                                                      #
                                                                                                                                  #
#-----                                                                                                                            #
df_outliers = df[(df['AnomalousDuration'] == True) | (df['AnomalousMovementDuration'] == True)]                                   #  
df = df[~((df['AnomalousDuration'] == True) | (df['AnomalousMovementDuration'] == True))]                                         #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Detectar registros de duración 0----------------------------------------------------------------------------------------------#
df_outliers = pd.concat([df_outliers,df_no_outliers[df_no_outliers['Duration'] == 0]])                                            #
df = df[~(df_no_outliers['Duration'] == 0)]                                                                                       #
df = df.reset_index(drop = True)                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Detectar interacciones 0------------------------------------------------------------------------------------------------------#
df_outliers = pd.concat([df_outliers, df[(df['MovementInteractions'] == 0) & (df['ArkboxInteractions'] == 0)]])                   #
df = df[~((df['MovementInteractions'] == 0) & (df['ArkboxInteractions'] == 0))]                                                   #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Detectar registros de duración de movimiento 0 con interacciones de movimiento > 0--------------------------------------------#
df_outliers = pd.concat([df_outliers,df[(df['MovementDuration']==0) & (df['MovementInteractions']>0)]])                           #
df = df[~((df['MovementDuration']==0) & (df['MovementInteractions']>0))]                                                          #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Detectar registros donde duración es igual a duración de movimiento e interacción con maravilloso módulo >0-------------------#
diff = df_no_outliers['Duration'] - df['MovementDuration']                                                                        #
df_outliers = pd.concat(df_outliers,df[(diff == 0) & (df['ArkboxInteractions'] > 0)])                                             #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 #
#***********************************************FIN LIMPIAR DATOS*****************************************************************#

#***********************************************ANÁLISIS**************************************************************************#
#                                                                                                                                 *
#---Retoques a datos previo a responder preguntas---------------------------------------------------------------------------------#
df_info = df[['Duration','MovementDuration','MovementInteractions','ArkboxInteractions','Key','Date','WeekDay',                   #
                          'Hour']].rename(columns = {'Key':'Potkey'}).set_index('Potkey',drop = True)                             #
                                                                                                                                  #
df_info['Hour'] = df_info['Hour'].astype('int64')                                                                                 #
df_info['WeekDay'] = df_info['WeekDay'].map({'Monday':'Weekday',                                                                  #
                                             'Tuesday':'Weekday',                                                                 #
                                             'Wednesday':'Weekday',                                                               #
                                             'Thursday':'Weekday',                                                                #
                                             'Friday':'Weekday',                                                                  #
                                             'Saturday':'Weekend',                                                                #
                                             'Sunday':'Weekend'})                                                                 #
                                                                                                                                  #
for k,i in enumerate(df_info['Hour']):                                                                                            #
    if i >= 5 and i <= 11:                                                                                                        #
        df_info.loc[df_info.index[k],'TimeofDay'] = '05:00 - 11:59 Morning'                                                       #
    elif i >= 12 and i <= 16:                                                                                                     #
        df_info.loc[df_info.index[k],'TimeofDay'] = '12:00 - 16:59 Afternoon'                                                     #
    elif i >= 17 and i <= 20:                                                                                                     #
        df_info.loc[df_info.index[k],'TimeofDay'] = '17:00 - 20:59 Evening'                                                       #
    elif i >=21 or i <= 4:                                                                                                        #
        df_info.loc[df_info.index[k],'TimeofDay'] = '21:00 - 4:59 Night'                                                          #                                                                                                                                           #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---Crear tablas que serán usadas para contestar preguntas------------------------------------------------------------------------#
other_1 = pd.read_sql("SELECT p.Potkey, c.Name FROM Pots p INNER JOIN Cities c ON p.CityId = c.CityId",                           #
                      my_conn).set_index('Potkey', drop = True)                                                                   #
                                                                                                                                  #
other_2 = df_pots[['PotKey','Serial']].rename(columns = {'PotKey':'Potkey'}).set_index('Potkey', drop = True)                     #
                                                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---¿Cuál es el top 10 de las ciudades con más movimientos registrados por los usuarios?------------------------------------------#
df_preg1 = df_info.join(other = other_1, on = 'Potkey', how = 'left')[['Name','MovementInteractions']].\                          #
groupby('Name')[['MovementInteractions']].sum().sort_values(by = 'MovementInteractions', ascending = False).head(10)              #
                                                                                                                                  #
df_preg1.to_csv(r'Documents\Prueba Tekus\preg1.csv')                                                                              #
                                                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#-¿Cuál es el top 10 de las ollas con más interacciones de los usuarios con el extraordinario panel interactivo de un solo botón?-#
df_preg2 = df_info.join(other = other_2, on = 'Potkey', how = 'left')[['Serial','ArkboxInteractions']].\                          #
groupby('Serial')[['ArkboxInteractions']].sum().sort_values(by = 'ArkboxInteractions', ascending = False).head(10)                #
                                                                                                                                  #
df_preg2.to_csv(r'Documents\Prueba Tekus\preg2.csv')                                                                              #
                                                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#
#                                                                                                                                 *
#---¿Cuáles son los horarios entre semana y fines de semana en dónde se presentan más desplazamientos de ollas?-------------------#
df_preg3 = df_info.groupby(['WeekDay','TimeofDay'])[['MovementInteractions']].sum()                                               #
                                                                                                                                  #
df_preg3.to_csv(r'Documents\Prueba Tekus\preg2.csv')                                                                              #
                                                                                                                                  #
#---------------------------------------------------------------------------------------------------------------------------------#