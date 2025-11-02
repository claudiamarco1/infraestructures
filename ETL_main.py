from Functions import api_etl, transform, make_plots

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

    # Función para generar las estadísticas y plots
    make_plots(df_clean)
    
    print("✅ ETL Completada con Exito!")
    
# --- Ejecutar el script ---
if __name__ == "__main__":
    run_etl()
