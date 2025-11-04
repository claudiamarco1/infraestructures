import subprocess, sys, os

"""
print("Current working directory:", os.getcwd())
def install_requirements():
    script_dir = os.path.dirname(os.path.abspath(__file__))  # folder where ETL_run.py is
    req_path = os.path.join(script_dir, "requirements.txt")
    
    if os.path.exists(req_path):
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_path])
        print("Requirements.txt file lista de packages instalados!")
    else:
        print("No se encuentra el archivo Requirements.txt. Instalación fallida!")
install_requirements()
"""

from Functions_v1 import api_etl, transform, load_sqlite3_db, make_plots

def run_etl():
    # API Link
    url = "https://randomuser.me/api"
    # Numero de Usuarios a Extraer
    users = 200
    # Valor para generar el mismo set de usuarios.
    fixed = "1234"

    # Data devuelve un JSON file de todos los usuarios
    data = api_etl(url, results = users,seed = fixed)

    # Función para Transformar los datos y limpiarlos
    df_clean = transform(data)

    # Función para cargar los datos en sqlite3 DB
    db_name="usuarios.db"
    table_name="usuarios"
    load_sqlite3_db(df_clean,db_name,table_name)
    
    # Función para generar las estadísticas y plots
    output_dir_name = "Resultados"
    make_plots(df_clean,output_dir_name)
    
    print("✅ ETL Completada con Exito!")
    
# --- Ejecutar el script ---
if __name__ == "__main__":
    run_etl()