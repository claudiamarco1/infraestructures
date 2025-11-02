import requests
import json
import statistics
import csv
import matplotlib.pyplot as plt
import random
import pandas as pd
import os
import folium
import webbrowser
import pathlib
import sys
import subprocess
from collections import Counter

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

random.seed(1234)

def run_etl():
    """
    Ejecuta un proceso ETL completo desde la API randomuser.me
    pidiendo 500 usuarios usando el argumento 'params'.
    """
    
    # Fijar semilla para reproducibilidad
    random.seed(1234)

    # --- 1. EXTRACCIÓN (Extract) ---
    
    print("Iniciando ETL...")
    num_users = 500
    print(f"1. Extrayendo datos de la API ({num_users} usuarios)...")
    
    # URL base de la API
    api_url = "https://randomuser.me/api/"
    
    # Parámetros para la solicitud
    # Esto es más limpio que concatenar strings en la URL
    params = {
        'results': num_users
    }
    
    try:
        # Pasamos la URL base y los parámetros por separado
        response = requests.get(api_url, params=params)
        
        # Lanza un error si la solicitud falla (ej. error 404, 500)
        response.raise_for_status() 
        data = response.json()
        users_raw = data.get('results', [])
        
        if not users_raw:
            print("No se recibieron datos de usuarios. Abortando.")
            return

        print(f"Datos extraídos: {len(users_raw)} usuarios.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error en la extracción: {e}")
        return

    # --- 2. TRANSFORMACIÓN (Transform) ---
    
    print("2. Transformando datos...")
    
    # 2a. Aplanar JSON a un DataFrame de Pandas
    try:
        df = pd.json_normalize(users_raw)
        
        # Seleccionamos y renombramos las columnas que nos interesan
        columns_map = {
            'gender': 'Genero',
            'name.first': 'Nombre',
            'name.last': 'Apellido',
            'dob.age': 'Edad',
            'location.country': 'Pais'
        }
        
        # Filtramos solo las columnas que existen en nuestro mapeo
        # (algunos campos como 'id.value' podrían ser útiles, pero nos ceñimos a lo básico)
        
        # Obtenemos las columnas disponibles que están en nuestro mapa
        available_columns = [col for col in columns_map.keys() if col in df.columns]
        
        df_clean = df[available_columns].copy()
        df_clean.rename(columns=columns_map, inplace=True)
        
        print("Datos limpiados y estructurados en un DataFrame.")
        print(df_clean.head()) # Muestra las primeras 5 filas

    except Exception as e:
        print(f"Error al transformar los datos: {e}")
        return

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
    
    # Creamos un directorio para guardar los resultados si no existe
    output_dir = "resultados_etl"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    

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
    df_clean['Registered'] = df['registered.age'] if 'registered.age' in df.columns else pd.Series([random.randint(0,10) for _ in range(len(df_clean))])

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
    
    
    # Grafico 5: Heatmap
    print("Generando mapa de usuarios por país...")
    conteo = Counter(u["location"]["country"] for u in users_raw)
    df = pd.DataFrame(conteo.items(), columns=["country", "count"])

    # GeoJSON de países
    url = "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json"
    world = requests.get(url).json()

    # Instalar pycountry si no está
    use_iso3 = True
    try:
        import pycountry
    except ModuleNotFoundError:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pycountry"])
            import pycountry
        except Exception:
            print("pycountry no disponible, usaré coincidencia por nombre.")
            use_iso3 = False

    def to_iso3(name):
        try:
            return pycountry.countries.lookup(name).alpha_3
        except:
            return None

    # Mapeo nombres especiales
    name_map = {
        "United States": "United States of America",
        "Russia": "Russian Federation",
        "Iran": "Iran (Islamic Republic of)",
        "Syria": "Syrian Arab Republic",
        "Moldova": "Republic of Moldova",
        "Tanzania": "United Republic of Tanzania",
        "Vietnam": "Viet Nam",
        "Laos": "Lao People's Democratic Republic",
        "South Korea": "Korea, Republic of",
        "North Korea": "Korea, Democratic People's Republic of",
        "Cape Verde": "Cabo Verde",
        "Ivory Coast": "Côte d'Ivoire",
        "Czechia": "Czech Republic",
        "Swaziland": "Eswatini",
        "The Bahamas": "Bahamas",
        "The Gambia": "Gambia",
        "Burma": "Myanmar",
        "North Macedonia": "Macedonia",  
        "Venezuela": "Venezuela (Bolivarian Republic of)",
        "Bolivia": "Bolivia (Plurinational State of)",
        "Micronesia": "Micronesia (Federated States of)",
        "Brunei": "Brunei Darussalam",
    }

    # Crear mapa
    m = folium.Map(location=[20, 0], zoom_start=2, tiles="OpenStreetMap")

    # Usar coincidencia por nombre en vez de ISO3
    df["country_corrected"] = df["country"].apply(lambda x: name_map.get(x, x))
    folium.Choropleth(
        geo_data=world,
        data=df,
        columns=["country_corrected", "count"],
        key_on="feature.properties.name",  # Coincide con nombre del GeoJSON
        fill_color="YlOrRd",
        nan_fill_color="#eeeeee",
        legend_name="Usuarios por país",
    ).add_to(m)

    # Guardar HTML y abrir
    out = "choropleth_usuarios.html"
    m.save(out)
    ruta = os.path.abspath(out)
    print("Mapa por países generado en HTML:", ruta)
    webbrowser.open(pathlib.Path(ruta).as_uri())

# --- Ejecutar el script ---
if __name__ == "__main__":
    run_etl()
