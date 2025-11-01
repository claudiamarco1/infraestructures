import requests
import json
import statistics
import csv
import matplotlib.pyplot as plt
import random
# EXTRACCIÓN: Descargar 500 usuarios
params = {"results": 500} # Pedimos 500 usuarios
url = "https://randomuser.me/api/"  
response = requests.get(url, params=params)
data = response.json()
usuarios = data["results"]
# TRANSFORMACIÓN: Calcular estadísticas
total_usuarios = len(usuarios)

# Lista de edades
edades = [u["dob"]["age"] for u in usuarios]

# Media de edad
media_edad = round(statistics.mean(edades), 2)

# Porcentaje de hombres y mujeres
generos = [u["gender"] for u in usuarios]
num_hombres = generos.count("male")
num_mujeres = generos.count("female")

porcentaje_hombres = round((num_hombres / total_usuarios) * 100, 2)
porcentaje_mujeres = round((num_mujeres / total_usuarios) * 100, 2)

# Calcular usuarios por país
paises = {}
for u in usuarios:
    pais = u["location"]["country"]
    paises[pais] = paises.get(pais, 0) + 1

pais_mas_frecuente = max(paises, key=paises.get)

# Crear diccionario con estadísticas
estadisticas = {
    "total_usuarios": total_usuarios,
    "media_edad": media_edad,
    "porcentaje_hombres": porcentaje_hombres,
    "porcentaje_mujeres": porcentaje_mujeres,
    "pais_mas_frecuente": pais_mas_frecuente
}
# CARGA: Crear tabla (lista de diccionarios)
tabla_usuarios = []

for u in usuarios:
    fila = {
        "nombre": f"{u['name']['first']} {u['name']['last']}",
        "edad": u["dob"]["age"],
        "genero": u["gender"],
        "pais": u["location"]["country"],
        "email": u["email"]
    }
    tabla_usuarios.append(fila)

# GUARDAR RESULTADOS EN ARCHIVOS
# Guardar estadísticas en JSON
with open("estadisticas.json", "w") as f:
    json.dump(estadisticas, f, indent=4)

# Guardar usuarios en JSON
with open("usuarios.json", "w") as f:
    json.dump(tabla_usuarios, f, indent=4)

# Guardar usuarios en CSV
with open("usuarios.csv", "w", newline='', encoding="utf-8") as csvfile:
    campos = ["nombre", "edad", "genero", "pais", "email"]
    writer = csv.DictWriter(csvfile, fieldnames=campos)
    writer.writeheader()
    writer.writerows(tabla_usuarios)

# VISUALIZACIÓN: Crear un gráfico
# Gráfico de distribución por género
plt.figure(figsize=(6, 6))
plt.bar(["Hombres", "Mujeres"], [num_hombres, num_mujeres], color=["blue", "pink"])
plt.title("Distribución de Género (500 usuarios)")
plt.xlabel("Género")
plt.ylabel("Número de Usuarios")

# Guardar gráfico como PNG
plt.savefig("grafico_genero.png")
plt.close()

# --- Gráfico 2: Top 10 países con más usuarios ---
# Ordenar los países de mayor a menor número de usuarios
paises_ordenados = sorted(paises.items(), key=lambda x: x[1], reverse=True)[:10]
paises_top = [p[0] for p in paises_ordenados]
valores_top = [p[1] for p in paises_ordenados]

plt.figure(figsize=(10, 6))
plt.barh(paises_top[::-1], valores_top[::-1], color="seagreen")  # horizontal y orden invertido
plt.title("Top 10 Países con Más Usuarios")
plt.xlabel("Número de Usuarios")
plt.ylabel("País")
plt.tight_layout()
plt.savefig("grafico_paises.png")
plt.close()

# --- Gráfico 3: Gráfico de puntos (edad por género) ---
edades_hombres = [u["dob"]["age"] for u in usuarios if u["gender"] == "male"]
edades_mujeres = [u["dob"]["age"] for u in usuarios if u["gender"] == "female"]

# Crear ejes X (posición) solo para visualización
x_hombres = [random.uniform(0.9, 1.1) for _ in edades_hombres]
x_mujeres = [random.uniform(1.9, 2.1) for _ in edades_mujeres]

plt.figure(figsize=(7, 6))
plt.scatter(x_hombres, edades_hombres, color="blue", alpha=0.5, label="Hombres")
plt.scatter(x_mujeres, edades_mujeres, color="pink", alpha=0.5, label="Mujeres")
plt.title("Distribución de Edad por Género (Gráfico de Puntos)")
plt.xticks([1, 2], ["Hombres", "Mujeres"])
plt.ylabel("Edad")
plt.xlabel("Género")
plt.legend()
plt.savefig("grafico_puntos.png")
plt.close()

# RESULTADOS EN CONSOLA
print("=== ESTADÍSTICAS ===")
for clave, valor in estadisticas.items():
    print(f"{clave}: {valor}")

print("\nArchivos generados correctamente:")
print(" - usuarios.csv")
print(" - usuarios.json")
print(" - estadisticas.json")
print(" - grafico_genero.png")
print(" - grafico_paises.png")
