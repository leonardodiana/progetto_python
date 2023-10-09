import sqlite3 as db
import pandas as pd
import numpy as np
import fastapi


#creo nuovo dataframe con arrivi e partenze per gli agriturismi
df_arrivi_agriturismo = pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/dati_turismo/Arrivi-negli-agriturismi-in-Italia-per-regione.csv', sep=';', encoding='latin1')
df_presenze_agriturismo = pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/dati_turismo/Presenze-negli-agriturismi-in-Italia-per-regione.csv', sep=';', encoding='latin1')
colonna_arrivi = df_arrivi_agriturismo['Arrivi'].to_numpy()
df_presenze_agriturismo.insert(3, "Arrivi", colonna_arrivi)

#creo nuovo dataframe con arrivi e partenze per gli campeggi
df_arrivi_campeggio = pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/dati_turismo/Arrivi-nei-campeggi-e-villaggi-turistici-in-italia-per-regione.csv', sep=';', encoding='latin1')
df_presenze_campeggio = pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/dati_turismo/Presenze-nei-campeggi-e-villaggi-turistici-in-italia-per-regione.csv', sep=';', encoding='latin1')
colonna_arrivi = df_arrivi_campeggio['Arrivi'].to_numpy()
df_presenze_campeggio.insert(3, "Arrivi", colonna_arrivi)
print(df_presenze_campeggio)

#creo nuovo dataframe per alberghi
df_arrivi_albergo = pd.read_csv('https://github.com/FabioGagliardiIts/datasets/blob/main/dati_turismo/Arrivi-negli-esercizi-alberghieri-in-Italia-per-regione.csv', sep=';', encoding='latin1')
df_presenze_albergo = pd.read_csv('https://github.com/FabioGagliardiIts/datasets/blob/main/dati_turismo/Presenze-negli-esercizi-alberghieri-in-Italia-per-regione.csv', sep=';', encoding='latin1')
colonna_arrivi = df_arrivi_albergo['Arrivi'].to_numpy()
df_presenze_albergo.insert(3, "Arrivi", colonna_arrivi)

# Creates or opens a file called mydb with a SQLite3 DB
db = db.connect('db.sqlite3')

##########
# CREATE #
##########
cursor = db.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS agriturismo(
        id INTEGER PRIMARY KEY,
        regione TEXT,
        anno INTEGER,
        arrivi INTEGER,
        presenze INTEGER
    )
''')
db.commit()

df_presenze_agriturismo.to_sql('agriturismo',db,if_exists='replace',index=False)
db.commit()

r_df = pd.read_sql("select * from agriturismo",db)
print(r_df)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS campeggio(
        id INTEGER PRIMARY KEY,
        regione TEXT,
        anno INTEGER,
        arrivi INTEGER,
        presenze INTEGER
    )
''')
db.commit()

df_presenze_campeggio.to_sql('campeggio',db,if_exists='replace',index=False)
db.commit()

r_df = pd.read_sql("select * from campeggio",db)
print(r_df)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS albergo(
        id INTEGER PRIMARY KEY,
        regione TEXT,
        anno INTEGER,
        arrivi INTEGER,
        presenze INTEGER
    )
''')
db.commit()

df_presenze_albergo.to_sql('campeggio',db,if_exists='replace',index=False)
db.commit()
