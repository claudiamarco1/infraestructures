import requests
import pandas as pd
import matplotlib.pyplot as plt
import os

def run_etl():
    """
    Ejecuta un proceso ETL completo desde la API randomuser.me
    pidiendo 500 usuarios usando el argumento 'params'.
    """
    
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
    
    print("--- Proceso ETL completado ---")

# --- Ejecutar el script ---
if __name__ == "__main__":
    run_etl()