import asyncio
import concurrent.futures
import datetime
import logging
import time

import pandas
import requests


# create logs
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("scraper.log", 'w')
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s %(asctime)s] "
                              "thread #%(thread)d - "
                              "at function %(funcName)s - "
                              "%(message)s",
                              "%H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)


def time_it(pattern):
    """Determine the execution time of the function"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            res = func(*args, **kwargs)
            print(pattern.format(time.time() - start_time))
            return res
        return wrapper
    return decorator


def get_n_parse(url, date):
    """Get html page with the desired data on the given date and parse it"""

    # create session
    session = requests.Session()

    try:
        # get the cookies
        init_payload = {"FLG_Autoconsulta": 1}
        session.post(url + "/SITLAPORWEB/InicioAplicacionPortal.do", data=init_payload)

        # refresh session
        session.get(url + "/SITLAPORWEB/AtPublicoViewAccion.do?tipoMenuATP=1")

        # construct request to retrieve html page with the desired data
        payload = {"TIP_Consulta": 5,
                   "TIP_Lengueta": "tdDos",
                   "SeleccionL": 0,
                   "ERA_Causa": 0,
                   "RUC_Tribunal": 4,
                   "FEC_Desde": date,
                   "FEC_Hasta": date,
                   "SEL_Trabajadores": 0,
                   "irAccionAtPublico": "Consulta",
                   "COD_Tribunal": 0}
        html_page = session.post(url + "/SITLAPORWEB/AtPublicoDAction.do", data=payload).text
    except requests.exceptions.ConnectionError:
        logger.exception("Can't access site", exc_info=False)
        html_page = ''

    # return parsed data
    try:
        return pandas.read_html(html_page, attrs={'id': 'filaSel'})
    except ValueError:
        logger.exception("Parsing error. No tables found", exc_info=False)
    except MemoryError:
        logger.exception("Have not enough memory to scrape", exc_info=False)


def gen_date(from_date, date_format, days_number):
    """Generate days in period from given date to nowadays"""

    for i in range(days_number + 1):
        yield datetime.date.strftime(from_date + datetime.timedelta(days=i), date_format)


async def scrape(url, from_date, date_format):
    """Scrape the data"""

    # set period for scraping in days_number
    from_date = datetime.datetime.strptime(from_date, date_format).date()
    to_date = datetime.date.today()
    days_number = abs(from_date - to_date).days + 1

    # yield day for each request
    days_generator = gen_date(from_date, date_format, days_number)

    # make async requests with multi threading
    with concurrent.futures.ThreadPoolExecutor(max_workers=days_number) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                get_n_parse,
                url, next(days_generator)
            )
            for _ in range(days_number)
        ]

        # [[df1], [df2], ..., [dfn]] => [df1, df2, ..., dfn]
        dataframes = [df for df_list in await asyncio.gather(*futures) if df_list is not None for df in df_list]

        # return concatenated dataframes
        return pandas.concat(dataframes, ignore_index=True)


@time_it("elapsed time: {} seconds")
def main():
    # initial params
    url = "https://laboral.pjud.cl"
    from_date = "22/12/2017"
    date_format = "%d/%m/%Y"

    # run async scraper
    logger.info("Start scraping")
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(scrape(url, from_date, date_format))
    loop.close()
    logger.info("Finish scraping")

    print(res)


main()
