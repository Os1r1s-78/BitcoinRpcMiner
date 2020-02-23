import pyodbc
from app import cfg

# Set up database connection
db = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + cfg.db['server'] + ';DATABASE=' + cfg.db['database'] +
    ';UID=' + cfg.db['username'] + ";PWD=" + cfg.db['password'])
cursor = db.cursor()


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
        'SELECT * FROM dbo.' + table + ' WHERE DataDay = (SELECT MAX(DataDay) FROM dbo.' + table + ')')
    row = cursor.fetchone()
    if row:
        return row.DataDay
    else:
        return None


def get_latest_tx_output():
    cursor.execute('SELECT * FROM dbo.TransactionOutputs WHERE Id = (SELECT MAX(Id) FROM dbo.TransactionOutputs)')
    row = cursor.fetchone()
    if row:
        return row
    else:
        return None


def delete_data_after_date(date, table):
    cursor.execute('DELETE FROM dbo.' + table + ' WHERE DataDay >= \'' + date.strftime('%Y-%m-%d') + '\'')
