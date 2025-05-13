# bitcoin_price.py

import requests
import json
import time
from datetime import datetime

# --- Configuración ---
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
OUTPUT_FILE = "bitcoin_price.json"
FETCH_INTERVAL_SECONDS = 10 # Consultar cada 10 segundos
CURRENCY = 'usd' # Moneda para el precio (usd, eur, etc.)

def get_bitcoin_price():
    """
    Consulta el precio actual de Bitcoin desde la API de CoinGecko.
    Retorna el precio como un float, o None si hay un error.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Lanza una excepción para errores HTTP (4xx o 5xx)
        data = response.json()
        price = data.get("bitcoin", {}).get(CURRENCY)
        if price is not None:
            return float(price)
        else:
            print(f"Error: No se pudo encontrar el precio de Bitcoin en {CURRENCY} en la respuesta: {data}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la API: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: La respuesta de la API no es un JSON válido.")
        return None
    except Exception as e:
        print(f"Un error inesperado ocurrió: {e}")
        return None

def determine_behaviour(current_price, last_price):
    """
    Determina el comportamiento del precio (Up, Down, Same).
    """
    if last_price is None or current_price == last_price:
        return "Same" # O "Initial" para el primer dato si se prefiere
    elif current_price > last_price:
        return "Up"
    else:
        return "Down"

def main():
    """
    Función principal que consulta precios y los escribe en el fichero JSON.
    """
    print(f"Iniciando la monitorización del precio de Bitcoin. Guardando en {OUTPUT_FILE}")
    print(f"Consultando cada {FETCH_INTERVAL_SECONDS} segundos. Presiona CTRL+C para detener.")

    last_price = None

    try:
        while True:
            current_price = get_bitcoin_price()

            if current_price is not None:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                behaviour = determine_behaviour(current_price, last_price)

                record = {
                    "Timestamp": timestamp,
                    "Behaviour": behaviour,
                    "Price": int(current_price) # El ejemplo usa precios enteros
                }

                try:
                    with open(OUTPUT_FILE, 'a') as f: # 'a' para append (añadir)
                        json.dump(record, f)
                        f.write('\n') # Asegura que cada JSON esté en una nueva línea
                    print(f"Guardado: {record}")
                except IOError as e:
                    print(f"Error al escribir en el fichero {OUTPUT_FILE}: {e}")

                last_price = current_price
            else:
                print("No se pudo obtener el precio en este intento. Reintentando...")

            time.sleep(FETCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nMonitorización detenida por el usuario.")
    finally:
        print("Proceso finalizado.")

if __name__ == "__main__":
    # Es buena idea borrar o renombrar el archivo existente al inicio para pruebas limpias
    # o añadir una lógica para manejarlo si es necesario.
    # Por simplicidad, este script simplemente añadirá al archivo si ya existe.
    main()