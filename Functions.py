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
    
    return df_clean


def make_plots(df_clean):
    """
    Función para calcular estadísticas y generar plots en formato png.
    """
    # 1. Create folder if it doesn't exist
    output_dir = "plots"
    os.makedirs(output_dir, exist_ok=True)

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


    # Gráfico 2: Distribución de Edades (Histograma)
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
    
    return
