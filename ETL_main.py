import subprocess, sys, os

# Importar las funciones
try:
    from Functions import api_extract, transform_data, load_and_visualize
except ImportError:
    print("Error al importar las funciones. Asegúrate de que 'Functions.py' está en el mismo directorio.")
    sys.exit(1)
except ModuleNotFoundError:
    print("\nError: Módulo no encontrado.")
    print("Asegúrate de que 'Functions.py' está en el mismo directorio.")
    print("Y de que has instalado las dependencias manualmente ejecutando:")
    print("pip install -r requirements.txt\n")
    sys.exit(1)

def run_etl():
    # API Link
    url = "https://randomuser.me/api"
    # Numero de Usuarios a Extraer
    users = 1000
    # Valor para generar el mismo set de usuarios.
    fixed = "1234"

    # Data devuelve un JSON file de todos los usuarios
    data = api_etl(url, results = users,seed = fixed)

    # Función para Transformar los datos y limpiarlos
    df_clean = transform(data)

    # Función para generar las estadísticas y plots
    make_plots(df_clean)
    
    print("ETL Completada con Exito!")
    
# --- Ejecutar el script ---
if __name__ == "__main__":
    run_etl()
