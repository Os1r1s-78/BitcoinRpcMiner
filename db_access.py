import psycopg2
from app import cfg
from app import logging

db, cursor = None, None


def setup_db():
    # Set up database connection
    logging.info('Trying to connect to the database')
    try:
        global db, cursor
        db = psycopg2.connect(
            host=cfg.db['server'],
            database=cfg.db['database'],
            user=cfg.db['username'],
            password=cfg.db['password'],
            port=cfg.db['port']
        )
        cursor = db.cursor()
        logging.info('Successfully connected to the database')
        return True
    except (KeyboardInterrupt, SystemExit) as e:
        logging.critical('Application interrupted, exiting: ' + repr(e))
        raise
    except Exception as e:
        print('Error while trying to connect to the database: ' + repr(e))
        logging.critical(repr(e))
        return False


def get_latest_active_day():
    freq = __get_latest_analysis('FrequencyAnalysis')
    size = __get_latest_analysis('SizeAnalysis')
    prot = __get_latest_analysis('ProtocolAnalysis')

    dates = []
    if freq is not None:
        dates.append(freq)
    if size is not None:
        dates.append(size)
    if prot is not None:
        dates.append(prot)

    return min(dates) if len(dates) > 0 else None


def __get_latest_analysis(table):
    cursor.execute(
        'SELECT * FROM ' + table + ' WHERE DataDay = (SELECT MAX(DataDay) FROM ' + table + ')')
    row = cursor.fetchone()
    if row:
        return row[1]
    else:
        return None


def get_latest_tx_output():
    cursor.execute('SELECT * FROM TransactionOutputs WHERE Id = (SELECT MAX(Id) FROM TransactionOutputs)')
    row = cursor.fetchone()
    if row:
        return row
    else:
        return None


def delete_data_after_date(date, table):
    cursor.execute('DELETE FROM ' + table + ' WHERE DataDay >= \'' + date.strftime('%Y-%m-%d') + '\'')
