import requests
import os
import csv
import datetime
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler('logfile.log')
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

max_files_process = 60000

def download_json_from_url(url):
    """
    Descarga el archivo JSON desde una URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status() 
        
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"Tipo de contenido recibido: {content_type}")
        
        logger.info(f"Contenido descargado: {response.text[:500]}")
        
        if 'json' in content_type:
            return response.json()
        else:
            logger.error("El contenido no es JSON.")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al descargar el JSON desde {url}: {e}")
        return None


def json_to_csv_writer(json_data, csv_writer_historical, csv_writer_current, processed_files, failed_files, file_count):
    """
    Procesa el JSON y escribe los datos extraídos en dos archivos CSV: uno histórico (con FechaRegistro) y otro actual (sin FechaRegistro).
    """
    unique_id = "from_url"
    reg_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    file_count[0] += 1

    """
    if unique_id in processed_files:
        logger.info(f'Archivo ya procesado anteriormente. Omitiendo...')
        return
    """

    try:
        logger.info(f"Procesando JSON desde URL")

        estaciones = json_data.get("ListaEESSPrecio", [])
        
        headers = set()
        for estacion in estaciones:
            headers.update(estacion.keys())

        headers = sorted(headers)

        if file_count[0] == 1:
            csv_writer_historical.writerow(['FechaRegistro'] + headers) 
            csv_writer_current.writerow(headers) 

        for estacion in estaciones:
            row_with_date = [reg_date]  
            row_without_date = []
            for header in headers:
                value = estacion.get(header, "")
                row_with_date.append(value) 
                row_without_date.append(value) 
            csv_writer_historical.writerow(row_with_date)  
            csv_writer_current.writerow(row_without_date) 

        processed_files.add(unique_id)
        logger.info(f"JSON procesado con éxito.")

    except Exception as e:
        logging.error(f"Error inesperado en JSON: {e}")
        failed_files.add(f"JSON desde URL - {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")


def process_from_url(url, csv_file_historical_path, csv_file_current_path, processed_files_path, failed_files_path, file_count):
    """
    Procesa el JSON desde la URL y escribe los resultados en dos archivos CSV: uno histórico (con FechaRegistro) y otro actual (sin FechaRegistro).
    """
    processed_files = set()
    failed_files = set()

    if os.path.exists(processed_files_path):
        with open(processed_files_path, 'r', encoding='utf-8') as file:
            processed_files = set(file.read().splitlines())

    with open(csv_file_historical_path, 'a', newline='', encoding='utf-8') as csv_file_historical, \
         open(csv_file_current_path, 'w', newline='', encoding='utf-8') as csv_file_current:

        csv_writer_historical = csv.writer(csv_file_historical)
        csv_writer_current = csv.writer(csv_file_current)
        
        logger.info(f"Iniciando el procesamiento desde la URL: {url}")
        json_data = download_json_from_url(url)
        if json_data:
            json_to_csv_writer(json_data, csv_writer_historical, csv_writer_current, processed_files, failed_files, file_count)

    with open(processed_files_path, 'w', encoding='utf-8') as file:
        for file_path in processed_files:
            file.write(file_path + '\n')

    with open(failed_files_path, 'w', encoding='utf-8') as file:
        for file_path in failed_files:
            file.write(file_path + '\n')


def main():
    
    url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
    file_count = [0] 
    
    process_from_url(url, 'historico.csv', 'actual.csv', 'Archivos_Procesados.txt', 'Errores.txt', file_count)

    logger.info("Finalizando el script de procesamiento JSON desde URL a CSV.")


if __name__ == '__main__':
    main()

"""
Web del gobiierno : https://datos.gob.es/en/catalogo/e0 5068001-precio-de-carburantes-en-las-gasolineras-espanolas
"""