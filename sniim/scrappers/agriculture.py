from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from sniim.db.mongo import Mongoclient
from clint.textui import puts, colored, indent


BASE_SNIIM_URL = 'http://www.economia-sniim.gob.mx/NUEVO/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas'
BASE_CATEGORIES = [
    [
        'Frutas y Hortalizas',
        '/ConsultaFrutasYHortalizas.aspx',
        '/ResultadosConsultaFechaFrutasYHortalizas.aspx'
    ],
    [
        'Flores',
        '/ConsultaFlores.aspx?SubOpcion=5',
        '/ResultadosConsultaFechaFlores.aspx'
    ],
    [
        'Granos',
        '/ConsultaGranos.aspx?SubOpcion=6',
        '/ResultadosConsultaFechaGranos.aspx'
    ],
    [
        'Aceites',
        '/ConsultaAceites.aspx?SubOpcion=8',
        '/ResultadosConsultaFechaAceites.aspx'
    ]
]


BASE_SCHEMA = (
    'fecha',
    'presentacion',
    'origen',
    'destino',
    'precio_min',
    'precio_max',
    'precio_frec',
    'obs',
    'producto'
)


class DateRangeException(Exception):
    pass


class CategoryException(Exception):
    pass



class AgricultureMarketScrapper:
    base_url = BASE_SNIIM_URL

    def __init__(self, category_url=None, prices_url=None, custom_schema=None):
        self.total_records = 0
        self.inserted_records = 0

        if not category_url:
            raise CategoryException('Invalid url: The category url can not be None')

        self.category_url = category_url

        if not prices_url:
            raise CategoryException('Invalid url: The prices url can not be None')

        self.prices_url = prices_url

        if not custom_schema:
            self.data_schema = BASE_SCHEMA

    def scrape(self, start_date=None, end_date=None):
        self.total_records = 0
        self.inserted_records = 0

        if not start_date:
            raise DateRangeException('Date range invalid: The start date most be datetime object not None')

        if not end_date:
            raise DateRangeException('Date range invalid: The end date most be datetime object not None')

        return self.get_category_products_information(start_date, end_date)

    def get_category_products_information(self, start_date, end_date):
        prices = []
        category_page_response = requests.get(self.base_url + self.category_url)

        category_page = BeautifulSoup(
            category_page_response.content,
            features="html.parser"
        )

        html_product_list = category_page.select_one('select#ddlProducto').find_all('option')
        products = [(product.getText(), product['value'],) for product in html_product_list]

        for product in products:
            product_name, product_id = product

            if product_id == '-1':
                continue

            with indent(4):
                puts(colored.magenta("Producto: {}".format(str(product_name))))

            # today = datetime.today()
            # start_date = today - timedelta(days=3)

            prices.extend(
                list(
                    self.get_product_prices(
                        product_id,
                        product_name,
                        start_date=start_date,
                        end_date=end_date
                    )
                )
            )

        return prices

    def get_product_prices(self, product_id, product_name, start_date=datetime.now(),
            end_date=datetime.now(), prices_per_id='2', rows_per_page='1000', destiny_id='-1'):

        get_params = {
            'fechaInicio': start_date.strftime('%d/%m/%Y'),
            'fechaFinal': end_date.strftime('%d/%m/%Y'),
            'ProductoId': product_id,
            'OrigenId': '-1',
            'Origen': 'Todos',
            'DestinoId': destiny_id,
            'Destino': 'Todos',
            'PreciosPorId': prices_per_id,
            'RegistrosPorPagina': rows_per_page
        }

        prices_response = requests.get(
            '{0}{1}'.format(self.base_url, self.prices_url),
            params=get_params
        )

        if prices_response.status_code != 200:
            return []

        html_prices = BeautifulSoup(prices_response.content, features="html.parser")
        table_prices = html_prices.select_one('table#tblResultados')

        # Traversing all the raws in the table
        for index_table, observation in enumerate(table_prices.find_all('tr')):
            print(index_table)
            if index_table > 1:
                td_enumerate = enumerate(observation.find_all('td'))
                price_row = {self.data_schema[metric_index]: metric.getText() for metric_index, metric in td_enumerate}
                price_row['producto'] = product_name
                self.total_records += 1

                yield price_row
