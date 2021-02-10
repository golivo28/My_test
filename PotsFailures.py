#ANÁLISIS COMPLEMENTARIO

import pandas as pd
import numpy as np

def failures(df_outliers, df_info, other_2):
    df_outliers = df_outliers.rename(columns = {'Key':'Potkey'})[['Duration','MovementDuration','MovementInteractions',
                                                             'ArkboxInteractions','Potkey',
                                                             'FailureType']].set_index('Potkey',drop = True)
    
    #¿Qué y cuántas fallas se presentaron?
    a = df_outliers.groupby('FailureType')[['Duration']].count().rename(columns = {'Duration':'Count'})
    a.to_excel(r'Documents\Prueba Tekus II\fail1.xlsx')
    
    #Proporción de fallas por olla
    df_fail = df_outliers.join(other = other_2, on = 'Potkey').groupby('Serial')[['Duration']].count().\
    rename(columns = {'Duration':'Count'}).sort_values(by = 'Count', ascending = False)

    df_good = df_info.join(other = other_2, on = 'Potkey').groupby('Serial')[['Duration']].count().\
    rename(columns = {'Duration':'Count'}).sort_values(by = 'Count', ascending = False)

    df_percentage = df_fail.join(other = df_good, on = 'Serial', lsuffix = '_fail', rsuffix = '_good')

    df_percentage['Failure rate (%)'] = np.round(100*df_percentage['Count_fail']/(df_percentage['Count_fail'] +\
                                                                                  df_percentage['Count_good']),2)

    df_percentage = df_percentage.sort_values(by = 'Failure rate (%)', ascending = False)

    df_percentage.to_excel(r'Documents\Prueba Tekus II\fail2.xlsx')
    
    