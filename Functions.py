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

    response = requests.get(url, params=params)
    response.raise_for_status()   # Si requests falla nos da información con un mensaje de error

    return response.json()
