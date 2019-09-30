from psycopg2 import Error
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sys import stdout
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

from pathlib import Path
import configparser
CONFIG = configparser.ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg')))
dbset = CONFIG['DBSETTINGS']
TCP = ThreadedConnectionPool(4,6, **dbset)

FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger('routing imputed')

# dates_to_route = [(datetime(2016,10,20), datetime(2016,10,20) + relativedelta(days=1), 'trip_data_routing_20161020_ext', 'routed_20161020'),
#                   (datetime(2017,3,30),  datetime(2017,3,30)  + relativedelta(days=1), 'trip_data_routing_20170330_ext', 'routed_20170330')]
# (range of bins to route, trip table, routing results table)
dates_to_route = [(datetime(2018,9,13), datetime(2018,9,13) + relativedelta(days=1), 'trip_data_routing_20180913_imputed', 'routed_20180913_imputed')]

times = []

#Creates 5-minute bins
for start_date, end_date, from_table, to_table in dates_to_route:
    route_tx = start_date
    while route_tx < end_date: 
        times.append((route_tx, from_table, to_table))
        route_tx += relativedelta(minutes=5)

def route_for_bin(bin, from_table, to_table):
    '''Call to routing function which gets parallelized'''
    logger.info('routing table {} into {} for bin: {}'.format(from_table, to_table, bin))
    con = TCP.getconn()
    try:
        with con:
            with con.cursor() as cur:
                cur.execute('SELECT ptc.route_trips_5min(%s, %s, %s)', (bin, from_table, to_table))
    except Error as err:
        logger.error('Psycopg2 error for bin: {}'.format(bin))
        logger.error(err)
    # Return connection to connection pool, very important
    TCP.putconn(con)

#Multi-threading woo! Make sure max_workers matches the second parameter of ThreadedConnectionPool()
with ThreadPoolExecutor(max_workers=6) as executor:
    tasks = {executor.submit(route_for_bin, tx, from_table, to_table): tx for tx, from_table, to_table in times}
        
    for task_num in as_completed(tasks):
        logger.info('routing complete for: {}'.format(tasks[task_num]))
    