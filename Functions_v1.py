# Como llamar la funcion:
""" 
from Functions import api_etl

url = "https://randomuser.me/api"
users = 200
fixed = "1234"

# Data devuelve un JSON file de todos los usuarios
data = api_etl(url, results = users,seed = fixed)
"""

import requests
import pandas as pd
import matplotlib.pyplot as plt
import os
import sqlite3

import statistics
import csv
import random
import folium
import webbrowser
import pathlib
import sys
import subprocess
from collections import Counter

        
def api_etl(url: str, results: int, seed: str):
    """
    Función para extraer los datos de dentro de randomuser.me API y devolverlos en formato JSON.

    Parameters
    ----------
    url : str
        API Link "https://randomuser.me/api"
    results : int
        Numero de usuarios a extraer (e.g., 500)
    seed : str
        Seed valor para generar el mismo set de usuarios.
    """
    params: Dict[str, str | int] = {
        "results": results, # El resultado de Nº users que queremos extraer
        "seed": seed,       # Seeds permite generar la misma seleccion de usuarios.
        "format": "json"    # JSON,CSV,XML,YAML output
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()   # Si requests falla nos da información con un mensaje de error
    
    except requests.exceptions.RequestException as e:    
        print(f"Error extrayendo los datos: {e}")
        
    return response.json()

def transform(data):
    """
    Transformar los datos JSON obtenidos, normalizar y devolver un DataFrame limpio.
    Extraer, seleccionar solo las columnas relevantes, renombrar y convertir los Datos.
    """
    
    # Normalizar los Datos JSON a Dataframe
    df = pd.json_normalize(data["results"])
    
    # Columns to rename
    rename_colls = {
        "gender": "Genero",
        "name.first": "Nombre",
        "name.last": "Apellido",
        "nat": "Nacionalidad",
        "dob.age": "Edad",
        "location.country": "Pais",
        "location.coordinates.latitude": "latitude",
        "location.coordinates.longitude": "longitude"
    }
    
    # Seleccionar solo columnas existentes
    columnas_seleccionadas = [col for col in rename_colls if col in df.columns]
    df_clean = df[columnas_seleccionadas]
    
    # Renombrar (sin asignar inplace)
    df_clean = df_clean.rename(columns=rename_colls)
    
    # Conversión de las columnas a variables int / float / datetime /category
    df_clean["Genero"] = df_clean["Genero"].astype("category")
    df_clean["Nacionalidad"] = df_clean["Nacionalidad"].astype("category")
    df_clean["Pais"] = df_clean["Pais"].astype("category")

    # Convertir lat/lon a numérico (si existen)  
    df_clean["latitude"] = pd.to_numeric(df_clean["latitude"], errors="coerce")
    df_clean["longitude"] = pd.to_numeric(df_clean["longitude"], errors="coerce")
    
    # Normalizamos la columna registered.age (años desde registro)
    df_clean['Registered'] = df['registered.age'] if 'registered.age' in df.columns else pd.Series([random.randint(0,10) for _ in range(len(df_clean))])
    
    return df_clean

def load_data():
    
    print(f"CSV Generated:{DB_name}")
    return

def load_sqlite3_db(df, db_name="usuarios.db", table_name="usuarios", data_load_type = "replace"):
    """
    Función para cargar los datos de Usuarios formato DataFrame en una base de datos SQLite.
    
    Parámetros:
        df (pandas.DataFrame): DataFrame con las columnas esperadas
        db_name (str): nombre del archivo .db (default 'usuarios.db')
        table_name (str): nombre de la tabla (default 'usuarios')
    """
    
    # Conectar a la base de datos, si no existe genera una nueva DB con el nombre default db_name="usuarios.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Crear la tabla si no existe, nombre default table_name="usuarios"
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Genero TEXT,
        Nombre TEXT,
        Apellido TEXT,
        Nacionalidad TEXT,
        Edad INTEGER,
        Pais TEXT,
        latitude REAL,
        longitude REAL
        );
    """
    )

    # Insertar el DataFrame (append = agrega datos sin borrar lo anterior)
    # if_exists="replace" Overwritte completely
    # data_load_type = append or replace
    df.to_sql(table_name, conn, if_exists=data_load_type, index=False)

    # Print Tables available
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables available: {tables}")
    
    # Guardar y cerrar conexión
    conn.commit()
    conn.close()

    #print(f"✅ Datos guardados correctamente en la tabla '{table_name}' dentro de '{db_name}'.")
    print("Datos Guardados correctamente! Sqlite3 DB & Table created!")
    print(f"DB name: {db_name}")
    print(f"DB Table name: {table_name}")
    return

def make_plots(df_clean, output_dir):
    """
    Función para calcular estadísticas y generar plots en formato png.
    """
    # 1. # Creamos un directorio para guardar los resultados si no existe
    #output_dir = "plots"
    os.makedirs(output_dir, exist_ok=True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    

    # 2b. Calcular Estadísticas
    print("Calculando estadísticas...")

    # Conteo por género
    gender_counts = df_clean['Genero'].value_counts()

    # Edad media total
    average_age = df_clean['Edad'].mean()

    # Edad media por género
    avg_age_by_gender = df_clean.groupby('Genero')['Edad'].mean()

    print(f"Edad media total: {average_age:.2f}")
    print(f"Conteo por género:\n{gender_counts}")
    
    # 2c. Generar Gráficos
    print("Generando gráficos...")
    
    # Gráfico 1: Distribución de Género (Barra)
    plt.figure(figsize=(8, 5))
    gender_counts.plot(kind='bar', color=['pink', 'skyblue'])
    plt.title('Distribución por Género (500 Usuarios)')
    plt.xlabel('Género')
    plt.ylabel('Cantidad')
    plt.xticks(rotation=0)
    plot_path_gender = os.path.join(output_dir, 'distribucion_genero.png')
    plt.savefig(plot_path_gender)
    plt.close()
    print(f"Gráfico de género guardado en: {plot_path_gender}")

    # Gráfico 1: Distribución de Edades (Histograma)
    plt.figure(figsize=(10, 6))
    df_clean['Edad'].plot(kind='hist', bins=20, edgecolor='black', color='lightgreen')
    plt.title('Distribución de Edades (500 Usuarios)')
    plt.xlabel('Edad')
    plt.ylabel('Frecuencia')
    # Añadimos una línea vertical para la edad media
    plt.axvline(average_age, color='red', linestyle='dashed', linewidth=2, label=f'Edad Media: {average_age:.2f}')
    plt.legend()
    plot_path_age = os.path.join(output_dir, 'distribucion_edad.png')
    plt.savefig(plot_path_age)
    plt.close()
    print(f"Gráfico de edad guardado en: {plot_path_age}")
    

    # Gráfico 2: Histograma de Nacionalidad
    print("Generando histograma de nacionalidades...")

    # Contar usuarios por país
    country_counts = df_clean['Pais'].value_counts()

    plt.figure(figsize=(12, 6))
    country_counts.plot(kind='bar', color='lightcoral')
    plt.title('Distribución de Usuarios por Nacionalidad')
    plt.xlabel('País')
    plt.ylabel('Cantidad de Usuarios')
    plt.xticks(rotation=90)
    plt.tight_layout()

    plot_path_country = os.path.join(output_dir, 'histograma_nacionalidad.png')
    plt.savefig(plot_path_country)
    plt.close()
    print(f"Histograma de nacionalidad guardado en: {plot_path_country}")

    # Gráfico 3: Gráfico Bivariante: Edad vs Años registrados
    print("Generando gráfico bivariante: Edad vs Años registrados...")

    # Normalizamos la columna registered.age (años desde registro)
    #df_clean['Registered'] = df['registered.age'] if 'registered.age' in df.columns else pd.Series([random.randint(0,10) for _ in range(len(df_clean))])

    plt.figure(figsize=(10, 6))
    plt.scatter(df_clean['Edad'], df_clean['Registered'], alpha=0.6, color='purple', edgecolors='w', s=50)
    plt.title('Edad del Usuario vs Años Registrado')
    plt.xlabel('Edad del Usuario')
    plt.ylabel('Años Registrado')
    plt.grid(True)
    plt.tight_layout()

    plot_path_bivar = os.path.join(output_dir, 'bivar_age_registered.png')
    plt.savefig(plot_path_bivar)
    plt.close()
    print(f"Gráfico bivariante guardado en: {plot_path_bivar}")


    # --- Crear columna de rango de edades ---
    print("Generando columna de rango de edades y histograma...")

    bins = [0, 18, 30, 65, 120]  # 120 para cubrir edades máximas posibles
    labels = ['0-18', '18-30', '30-65', '65+']

    df_clean['RangoEdad'] = pd.cut(df_clean['Edad'], bins=bins, labels=labels, right=False)

    # Conteo de usuarios por rango de edad
    rango_counts = df_clean['RangoEdad'].value_counts().sort_index()

    # Grafico 4: Histograma de usuarios por rango de edad
    plt.figure(figsize=(8, 5))
    rango_counts.plot(kind='bar', color='orange', edgecolor='black')
    plt.title('Número de Usuarios por Rango de Edad')
    plt.xlabel('Rango de Edad')
    plt.ylabel('Número de Usuarios')
    plt.xticks(rotation=0)
    plt.tight_layout()

    plot_path_rango = os.path.join(output_dir, 'histograma_rango_edad.png')
    plt.savefig(plot_path_rango)
    plt.close()
    print(f"Histograma de rango de edades guardado en: {plot_path_rango}")


    # --- 3. CARGA (Load) ---
    print("3. Cargando datos y estadísticas en ficheros...")
    
    # 3a. Cargar datos crudos de usuarios a CSV
    raw_data_path = os.path.join(output_dir, 'raw_users.csv')
    df_clean.to_csv(raw_data_path, index=False, encoding='utf-8')
    print(f"Datos crudos de usuarios guardados en: {raw_data_path}")

    # 3b. Cargar estadísticas a CSV
    stats_data = {
        'Metrica': [
            'Edad Media Total', 
            'Edad Media (male)', 
            'Edad Media (female)', 
            'Total (male)', 
            'Total (female)'
        ],
        'Valor': [
            f"{average_age:.2f}",
            f"{avg_age_by_gender.get('male', 0):.2f}",
            f"{avg_age_by_gender.get('female', 0):.2f}",
            gender_counts.get('male', 0),
            gender_counts.get('female', 0)
        ]
    }
    stats_df = pd.DataFrame(stats_data)
    stats_path = os.path.join(output_dir, 'statistics.csv')
    stats_df.to_csv(stats_path, index=False, encoding='utf-8')
    print(f"Estadísticas guardadas en: {stats_path}")
    
    return

