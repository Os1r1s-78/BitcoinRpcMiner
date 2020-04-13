# import psycopg2
# import psycopg2.extras
import time
import math
from app import cfg
from app import logging
import pyodbc
from dbmodels import TransactionOutput

db, cursor = None, None


# Splits a list into fixed-size chunks
def chunks(split, n):
    for i in range(0, len(split), n):
        yield split[i:i+n]


def setup_db():
    # Set up database connection
    logging.info('Trying to connect to the database')
    try:
        global db, cursor
        db = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + cfg.db['server'] + ';DATABASE=' +
                            cfg.db['database'] + ';UID=' + cfg.db['username'] + ';PWD=' + cfg.db['password'] + ';Trusted_Connection=Yes', autocommit=True)  # Add Trusted_Connection=Yes on Windows
        db.setencoding(encoding='utf-8')
        cursor = db.cursor()
        logging.info('Successfully connected to the database')
        return True
    except (KeyboardInterrupt, SystemExit) as e:
        logging.critical('Application interrupted, exiting: ' + repr(e))
        raise
    except Exception as e:
        logging.critical('Error while trying to connect to the database: ' + repr(e))
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
    if outputs is not None and len(outputs) > 0:
        insert = TransactionOutput.convert_to_list(outputs)
        insert_chunks = list(chunks(insert, 1000))
        for chunk in insert_chunks:
            if chunk is not None and len(chunk) > 0:
                query = 'INSERT INTO transactionoutputs (txhash, blocktime, blockhash, outvalue, outtype, outasm, outhex, protocol, fileheader) VALUES '
                for tx in chunk:
                    query += '(\'{0}\',{1},\'{2}\',{3},\'{4}\',\'{5}\',\'{6}\',\'{7}\',\'{8}\')'.format(tx[0], tx[1], tx[2], tx[3], tx[4], tx[5], tx[6], tx[7], tx[8])
                cursor.execute(query.rstrip(','))


def insert_freq_analysis(analysis):
    cursor.execute(
        'INSERT INTO frequencyanalysis (dataday, nulldata, p2pk, p2pkh, p2ms, p2sh, unknowntype) '
        'VALUES (\'{0}\', {1}, {2}, {3}, {4}, {5}, {6})'.format(
            analysis.dataday, analysis.nulldata, analysis.p2pk, analysis.p2pkh, analysis.p2ms, analysis.p2sh, analysis.unknowntype)
    )


def insert_size_analysis(analysis):
    cursor.execute(
        'INSERT INTO sizeanalysis (dataday, avgsize, outputs) '
        'VALUES (\'{0}\', {1}, {2})'.format(
            analysis.dataday, analysis.avgsize, analysis.outputs)
    )


def insert_file_analysis(analysis):
    cursor.execute(
        'INSERT INTO fileanalysis (dataday, avi_wav, bin_file, bpg, bz2, crx, dat, deb, doc_xls_ppt, docx_xlsx_pptx, '
        'dmg, exe_dll, flac, flv, gif, gz, iso, jpg, lz, mkv, mp3, mp4, ogg, pdf, png, psd, rar, rtf, seven_z, '
        'sqlite, tar, threegp, tiff, webp, wmv, xml, zip) '
        'VALUES (\'{0}\', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, '
        '{16}, {17}, {18}, {19}, {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29}, {30}, {31}, {32}, {33}, '
        '{34}, {35}, {36})'.format(
            analysis.dataday, analysis.avi_wav, analysis.bin_file, analysis.bpg, analysis.bz2, analysis.crx, analysis.dat,
            analysis.deb, analysis.doc_xls_ppt, analysis.docx_xlsx_pptx, analysis.dmg, analysis.exe_dll, analysis.flac,
            analysis.flv, analysis.gif, analysis.gz, analysis.iso, analysis.jpg, analysis.lz, analysis.mkv,
            analysis.mp3, analysis.mp4, analysis.ogg, analysis.pdf, analysis.png, analysis.psd, analysis.rar, analysis.rtf,
            analysis.seven_z, analysis.sqlite, analysis.tar, analysis.threegp, analysis.tiff, analysis.webp, analysis.wmv,
            analysis.xml, analysis.zip)
    )


def insert_prot_analysis(analysis):
    cursor.execute(
        'INSERT INTO protocolanalysis (dataday, ascribe, bitproof, blockaibindedpixsy, blocksign, blockstoreblockstack, '
        'chainpoint, coinspark, colu, counterparty, counterpartytest, cryptocopyright, diploma, emptytx, eternitywall, '
        'factom, lapreuve, monegraph, omni, openassets, openchain, originalmy, proofofexistence, provebit, remembr, '
        'smartbit, stampd, stampery, universityofnicosia, unknownprotocol, veriblock) '
        'VALUES (\'{0}\', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, '
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
