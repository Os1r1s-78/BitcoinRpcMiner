import psycopg2
import psycopg2.extras
import time
import math
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
        db.autocommit = True
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
    file = __get_latest_analysis('FileAnalysis')
    prot = __get_latest_analysis('ProtocolAnalysis')

    dates = []
    if freq is not None:
        dates.append(freq)
    else:
        return None

    if size is not None:
        dates.append(size)
    else:
        return None

    if file is not None:
        dates.append(file)
    else:
        return None

    if prot is not None:
        dates.append(prot)
    else:
        return None

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


def insert_tx_outputs(outputs):
    psycopg2.extras.execute_values(cursor, 'INSERT INTO transactionoutputs (id, txhash, blocktime, blockhash, outvalue, outtype, outasm, outhex, protocol, fileheader) VALUES %s', outputs)


def insert_freq_analysis(analysis):
    cursor.execute(
        'INSERT INTO frequencyanalysis (id, dataday, nulldata, p2pk, p2pkh, p2ms, p2sh, unknowntype) '
        'VALUES (default, \'{0}\', {1}, {2}, {3}, {4}, {5}, {6})'.format(
            analysis.dataday, analysis.nulldata, analysis.p2pk, analysis.p2pkh, analysis.p2ms, analysis.p2sh, analysis.unknowntype)
    )


def insert_size_analysis(analysis):
    cursor.execute(
        'INSERT INTO sizeanalysis (id, dataday, avgsize, outputs) '
        'VALUES (default, \'{0}\', {1}, {2})'.format(
            analysis.dataday, analysis.avgsize, analysis.outputs)
    )


def insert_file_analysis(analysis):
    cursor.execute(
        'INSERT INTO fileanalysis (id, dataday, fileheader) '
        'VALUES (default, \'{0}\', \'{1}\')'.format(
            analysis.dataday, analysis.fileheader)
    )


def insert_prot_analysis(analysis):
    cursor.execute(
        'INSERT INTO protocolanalysis (id, dataday, ascribe, bitproof, blockaibindedpixsy, blocksign, blockstoreblockstack, '
        'chainpoint, coinspark, colu, counterparty, counterpartytest, cryptocopyright, diploma, emptytx, eternitywall, '
        'factom, lapreuve, monegraph, omni, openassets, openchain, originalmy, proofofexistence, provebit, remembr, '
        'smartbit, stampd, stampery, universityofnicosia, unknownprotocol, veriblock) '
        'VALUES (default, \'{0}\', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, '
        '{16}, {17}, {18}, {19}, {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29}, {30})'.format(
            analysis.dataday, analysis.ascribe, analysis.bitproof, analysis.blockaibindedpixsy, analysis.blocksign, analysis.blockstoreblockstack,
            analysis.chainpoint, analysis.coinspark, analysis.colu, analysis.counterparty, analysis.counterpartytest, analysis.cryptocopyright,
            analysis.diploma, analysis.emptytx, analysis.eternitywall, analysis.factom, analysis.lapreuve, analysis.monegraph, analysis.omni,
            analysis.openassets, analysis.openchain, analysis.originalmy, analysis.proofofexistence, analysis.provebit, analysis.remembr,
            analysis.smartbit, analysis.stampd, analysis.stampery, analysis.universityofnicosia, analysis.unknownprotocol, analysis.veriblock)
    )


def delete_all_data():
    cursor.execute('DELETE FROM transactionoutputs; DELETE FROM frequencyanalysis; DELETE FROM sizeanalysis; DELETE FROM fileanalysis; DELETE FROM protocolanalysis;')


def delete_data_after_date(date, table):
    cursor.execute('DELETE FROM ' + table + ' WHERE DataDay >= \'' + date.strftime('%Y-%m-%d') + '\'')


def delete_tx_outputs_after_date(date):
    cursor.execute('DELETE FROM transactionoutputs WHERE blocktime >= ' + str(math.floor(time.mktime(date.timetuple()))))
