import requests
import requests.auth
from datetime import date, timedelta
from urllib.parse import urljoin
import logging
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

url = 'https://marmenor.upct.es/thredds/fileServer/L1/SERVICIODEPESCA/'

usuario = os.environ.get('MARMENOR_USER')
contrasena = os.environ.get('MARMENOR_PASS')

# Establecer una sesión HTTP
session = requests.Session()
session.auth = (usuario, contrasena)

download_path = 'E:SDC_Brutos/'


def get_dates():
    '''
    Crea un generador desde la primera medición (22-3-2017) hasta el día
    de ejecución
    :return: yield generador
    '''
    first_measure_date = date(day=22, month=3, year=2017)
    today = date.today()

    for n in range((today - first_measure_date).days+1):
        yield first_measure_date + timedelta(n)


def get_L1_url(dt):
    '''
    Pasa una fecha (date) y retorna la URL a la que descargar
    el producto L1 del SDC.
    :param dt: Fecha, en formato nativo date de Python
    :return: url a la que descargar los datos
    '''
    url_base = 'https://marmenor.upct.es/thredds/fileServer/L1/'\
        'SERVICIODEPESCA/'
    url_date = dt.strftime('%Y%m%d')
    return urljoin(url_base, f"{url_date}/data.nc")


def download(url, dt, download_path):
    date_str = dt.strftime('%Y-%m-%d')
    file_path = os.path.join(download_path, date_str + ".nc")

    if not os.path.exists(file_path):

        try:
            date = dt.strftime('%Y-%m-%d')
            response = session.get(url, stream=True)
            logging.info(
                "Fecha: " + date_str + ". " +
                "Código de estado: " + str(response.status_code))

            if str(response.status_code) == "200":
                with open(download_path + date + ".nc", "wb") as f:
                    f.write(response.content)

        except Exception as e:
            logging.error(f"Error al descargar los datos para {date_str}: {e}")
    else:
        logging.info(f"El archivo para {date_str} "
                     "ya existe. Omitiendo descarga.")


for dt in get_dates():
    url = get_L1_url(dt)
    download(url, dt, download_path=download_path)
