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
route = r'Documents\Prueba Tekus'                                                                                                 #
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

df = df[['Duration','MovementDuration','MovementInteractions','HardwareInteractions','Key','DatetimeFmt','DayofWeek','Time']]

df.rename(columns = {'DatetimeFmt':'Date',
                     'Key':'PotKey'},inplace = True)

#---------------------------


