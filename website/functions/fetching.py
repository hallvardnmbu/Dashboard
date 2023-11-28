"""Functions for fetching data from BarentsWatch and Frost APIs."""

import json
import requests
import pandas as pd
from logging import getLogger
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.utils import AnalysisException


# MODIFY THE FOLLOWING LINES TO MATCH YOUR OWN API CREDENTIALS
# *----------------------------------------------------------*

_FISH_ID = open('../../secrets/barentswatch_id', 'r').read()
_FISH_SECRET = open('../../secrets/barentswatch', 'r').read()
_FROST_ID = open('../../secrets/frost_id', 'r').read()

# *----------------------------------------------------------*


_FISH_TABLE = 'fish'
_LOCALITY_TABLE = 'locality'
_WEATHER_TABLE = 'weather'

_LON, _LAT = 15.3173, 68.3749
logger = getLogger(__name__)


# %% SPARK AND CASSANDRA INITIALISATION


_SPARK = SparkSession.builder.appName('SparkCassandraApp'). \
    config('spark.jars.packages',
           'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1'). \
    config('spark.cassandra.connection.host', 'localhost'). \
    config('spark.sql.extensions',
           'com.datastax.spark.connector.CassandraSparkExtensions'). \
    config('spark.sql.catalog.mycatalog',
           'com.datastax.spark.connector.datasource.CassandraCatalog'). \
    config('spark.cassandra.connection.port', '9042').getOrCreate()

cluster = Cluster(['localhost'], port=9042)
_SESSION = cluster.connect()
_SESSION.execute(
    "CREATE KEYSPACE IF NOT EXISTS streamsync "
    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
)
_SESSION.set_keyspace('streamsync')


# %% _BARENTSWATCH


_BW_URL = "https://www.barentswatch.no/bwapi"
_BW_QUERY = "/v1/geodata/fishhealth/locality/{}/{}"


class BarentsWatch:
    def __init__(self,
                 client_id, client_secret):
        """
        Class for fetching data from BarentsWatch API.

        Parameters
        ----------
        client_id : str
            Client ID for BarentsWatch API.
        client_secret : str
            Client secret for BarentsWatch API.
        """
        logger.info("Initializing BarentsWatch class.")

        try:
            token = requests.post(
                "https://id.barentswatch.no/connect/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "api"
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )
            self.headers = {
                "authorization": f"Bearer {token.json()['access_token']}",
                "content-type": "application/json"
            }
        except Exception as e:
            logger.error("Could not authenticate with BarentsWatch API.")
            logger.error(e)
            raise e

        self.lon = None
        self.lat = None
        self.data = None

    def download(self, from_year, to_year=None, locality=None,
                 table=_FISH_TABLE, primary_key='timestamp'):
        """
        Fetch data from BarentsWatch API.

        Parameters
        ----------
        from_year : int
            First year to fetch data from.
        to_year : int, optional
            Last year to fetch data from. If None, fetch data from only one year.
        locality : int, optional
            Locality number to fetch data from. If None, fetch data from all localities.
        table : str, optional
            Name of the table in the database.
        primary_key : str, optional
            Name of the primary key in the database.

        Returns
        -------
        pandas.DataFrame
            Dictionary with data from BarentsWatch API.

        Raises
        ------
        ValueError
            If to_year is less than from_year.

        Notes
        -----
        If locality is None, the data is fetched for all localities, and for the specific
        locality otherwise. The data is fetched for all weeks in the specified year(s). If the
        locality is specified, the data for liceCountPreviousWeek is returned, and if not,
        all data is returned.
        """
        from_year = int(from_year)
        to_year = int(to_year) if to_year else None

        logger.info(
            f"Starting data fetch for years {from_year} to {to_year} and locality {locality}.")

        to_year = from_year if to_year is None else to_year

        if to_year < from_year:
            logger.error("to_year must be greater than from_year. Using from_year as to_year.")
            to_year = from_year

        if locality is None:
            endings = [_BW_QUERY.format(year, week)
                       for year in range(from_year, to_year + 1)
                       for week in range(1, 52 + 1)]
        else:
            endings = [_BW_QUERY.format(str(locality) + "/" + str(year), week)
                       for year in range(from_year, to_year + 1)
                       for week in range(1, 52 + 1)]

        data = []
        for _ending in endings:
            _response = requests.get(_BW_URL + _ending, headers=self.headers)
            _response.raise_for_status()
            _raw = _response.json()

            if locality is None:
                for _locality in _raw["localities"]:
                    _locality.update({"year": _raw["year"],
                                      "week": _raw["week"]})
                    data.append(_locality)
            else:
                self.lon = _raw['aquaCultureRegister']['lon']
                self.lat = _raw['aquaCultureRegister']['lat']

                _raw = _raw["localityWeek"]

                for column in ["bathTreatments", "inFeedTreatments",
                               "cleanerFish", "mechanicalRemoval"]:
                    try:
                        if column in _raw:
                            _raw[column] = json.dumps(_raw[column])
                    except:  # noqa
                        pass

                data.append(_raw)

        self.data = pd.DataFrame(data)
        self.data.columns = self.data.columns.str.lower()
        self.data.fillna(value=0, inplace=True)

        self.data.index = pd.to_datetime(self.data['year'].astype(str) + '-W' +
                                         self.data['week'].astype(str) + '-1',
                                         format='%Y-W%U-%w')
        self.data.sort_index(inplace=True)

        logger.info("Data fetch completed successfully.")
        logger.info("Starting data save.")

        self.save(table, primary_key)

        logger.info("Data save completed successfully.")

    def save(self, table, primary_key):
        """
        Save data to a Cassandra database.

        Parameters
        ----------
        table : str, optional
            Name of the table in the database.
        primary_key : str, optional
        """
        logger.info(f"Starting data save to table {table}.")

        self.data.reset_index(inplace=True)
        self.data.rename(columns={'index': 'timestamp'}, inplace=True)

        types = {
            'int64': 'int',
            'float64': 'float',
            'object': 'text',
            'datetime64[ns]': 'timestamp',
            'bool': 'boolean'
        }
        columns = [f"{col} {types.get(str(dtype), 'TEXT')}"
                   for col, dtype in self.data.dtypes.iteritems()]

        _SESSION.execute(f"DROP TABLE IF EXISTS {table};")
        _SESSION.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}, "
            f"PRIMARY KEY({primary_key}));"
        )

        spark_dataframe = _SPARK.createDataFrame(self.data)
        spark_dataframe.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table, keyspace="streamsync") \
            .save()

        logger.info("Data save completed successfully.")

    @staticmethod
    def load(table=_FISH_TABLE):
        """Load data from a Cassandra database."""
        logger.info("Loading fish")

        data = _SPARK.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace="streamsync") \
            .load().toPandas()

        return data


# %% _FROST


_FROST_URL = 'https://frost.met.no/observations/v0.jsonld'


class Frost:
    def __init__(self, client_id, lon, lat, count=1):
        """
        Class for fetching data from Frost API.

        Parameters
        ----------
        client_id : str
            Client ID for Frost API.
        lon : float
        lat : float
        count : int, optional
            Number of stations to fetch data from.
        """
        self.client_id = client_id

        nearest = requests.get(
            'https://frost.met.no/sources/v0.jsonld',
            {'geometry': f'nearest(POINT({lon} {lat}))',
             'nearestmaxcount': count},
            auth=(self.client_id, '')
        )
        self.stations = ",".join([data['id'] for data in nearest.json()['data']])

        self.station_coordinates = {
            data['id']: data['geometry']['coordinates']
            for data in nearest.json()['data']
        }

        self.elements = self._elements()

        self.data = None
        self.start = None
        self.end = None

    def _elements(self):
        """Get all available elements from the Frost API."""
        url = 'https://frost.met.no/observations/availableTimeSeries/v0.jsonld'
        parameters = {
            'sources': self.stations,
        }
        _response = requests.get(url, parameters,
                                 auth=(self.client_id, ''))

        elements = {unique.get('level', {}).get('levelType', unique['elementId'])
                    for unique in _response.json()['data']}

        elements = [element for element in elements if
                    "P1M" not in element and
                    "P1Y" not in element and
                    "PT2M" not in element and
                    "P3M" not in element and
                    "P6M" not in element and
                    "PT1M" not in element and
                    "PT10M" not in element and
                    "PT12H" not in element and
                    "PT30D" not in element and
                    "P30D" not in element and
                    "over_time" not in element and
                    "PT6H" not in element]

        return ','.join(elements)

    def download(self, from_year, to_year=None,
                 table=_WEATHER_TABLE):
        """
        Fetch data from Frost API.

        Parameters
        ----------
        from_year : int
            First year to fetch data from.
        to_year : int, optional
            Last year to fetch data from.
        table : str, optional
            Name of the table in the database.
        """
        from_year = int(from_year)
        to_year = int(to_year) if to_year else None

        self.start = f"{from_year}-01-01"
        self.end = f"{to_year}-12-31" if to_year else f"{from_year}-12-31"

        start_date = pd.to_datetime(self.start)
        end_date = pd.to_datetime(self.end)

        date_range = pd.date_range(start=start_date, end=end_date,
                                   freq='50D').union([end_date])

        data = []
        for i in range(len(date_range) - 1):

            start = date_range[i].strftime('%Y-%m-%d')
            end = date_range[i + 1].strftime('%Y-%m-%d')

            date_range_str = f"{start}/{end}"

            parameters = {
                'sources': self.stations,
                'referencetime': date_range_str,
            }

            try:
                parameters['elements'] = self.elements

                _response = requests.get(_FROST_URL, parameters,
                                         auth=(self.client_id, ''))
                response = _response.json()

                data.append(pd.json_normalize(response['data'], record_path='observations',
                                              meta=['sourceId', 'referenceTime']))
            except requests.exceptions.JSONDecodeError:
                def _chunks(lst, n):
                    for _i in range(0, len(lst), n):
                        yield lst[_i:_i + n]

                _data = []
                for chunk in _chunks(self.elements.split(','), 5):
                    parameters['elements'] = ','.join(chunk)

                    _response = requests.get(_FROST_URL, parameters,
                                             auth=(self.client_id, ''))
                    response = _response.json()

                    try:
                        _data.append(pd.json_normalize(response['data'], record_path='observations',
                                                       meta=['sourceId', 'referenceTime']))
                    except:  # noqa
                        pass
                data.append(pd.concat(_data))
            except (KeyError, ValueError):
                logger.debug("No data found for specified time range.")
        try:
            self.data = pd.concat(data)

            self._clean()
            self.save(table)
        except ValueError:
            logger.warning("No data found for specified time range.")

    def _clean(self, remove=None, threshold=False):
        """
        Remove unnessecary columns of the data and combines the data so that the index is unique
        by transforming the rows into columns. The data is then resampled to weekly data.

        Parameters
        ----------
        remove : list, optional
            List of columns to remove.
        threshold : float, optional
            Threshold for the amount of NaN values a column can have before it is removed.
            Percentage, between 0 and 1.

        Notes
        -----
        The following columns are removed by default:
        - timeSeriesId
        - performanceCategory
        - exposureCategory
        - qualityCode
        - level.unit
        - level.levelType
        - level.value
        """
        if remove is None:
            remove = ["timeSeriesId", "performanceCategory", "exposureCategory",
                      "qualityCode", "level.unit", "level.levelType", "level.value",
                      "codeTable", "unit", "timeOffset", "timeResolution"]

        self.data.drop(columns=[col for col in remove if col in self.data.columns], inplace=True)

        self.data['referenceTime'] = pd.to_datetime(self.data['referenceTime'])
        self.data.set_index('referenceTime', inplace=True, drop=True)

        weeks = pd.date_range(start=self.start, end=self.end, freq='W', tz='UTC')

        grouped = self.data.groupby(["elementId", "sourceId"])
        weather = []

        for group, data in grouped:
            try:
                if data.index.duplicated().any():
                    data = data.groupby(data.index).mean()

                data.fillna(method='bfill', inplace=True)

                data = data.reindex(weeks, method='ffill').resample('W').mean()

                data = data.rename(columns={"value": group})
                weather.append(data)

            except ValueError as e:
                logger.warning(f"Could not resample data for {group}: {e}")

        self.data = pd.concat(weather, axis=1).dropna(axis=1, how='all')
        self.data.index = self.data.index.strftime('%Y-%m-%d')

        columns = ["_".join(x)
                   .replace(":0", "")
                   .replace("(", "_")
                   .replace(")P", "_P")
                   .replace(")p", "_p")
                   .replace(")", "")
                   .replace(" ", "_").lower()
                   for x in list(self.data.columns)]
        self.data.rename(columns=dict(zip(self.data.columns, columns)), inplace=True)

        if threshold:
            nans = self.data.isna().mean()
            self.data = self.data.loc[:, nans <= threshold]

    def save(self, table):
        """
        Save data to a Cassandra database.

        Parameters
        ----------
        table : str
            Name of the table in the database.
        """
        logger.info(f"Starting data save to table {table}.")

        self.data.index = pd.to_datetime(self.data.index)
        self.data.sort_index(inplace=True)

        self.data.reset_index(inplace=True)
        self.data.rename(columns={'index': 'timestamp'}, inplace=True)

        types = {
            'int64': 'int',
            'float64': 'float',
            'object': 'text',
            'datetime64[ns]': 'timestamp',
            'bool': 'boolean'
        }
        columns = [f"{col} {types.get(str(dtype), 'TEXT')}"
                   for col, dtype in self.data.dtypes.iteritems()]

        _SESSION.execute(f"DROP TABLE IF EXISTS {table};")
        _SESSION.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}, "
            f"PRIMARY KEY(timestamp));"
        )

        spark_dataframe = _SPARK.createDataFrame(self.data)
        spark_dataframe.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table, keyspace="streamsync") \
            .save()
        logger.info("Data save completed successfully.")

    @staticmethod
    def load(table=_WEATHER_TABLE):
        """Load data from a Cassandra database."""
        logger.info("Loading weather")

        df_weather = _SPARK.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace="streamsync") \
            .load().toPandas()

        return df_weather


# %% DOWNLOADING AND SAVING DATA


def _download_fish(state):
    _BARENTSWATCH.download(from_year=state['parameter']['from_year'],
                           to_year=state['parameter']['to_year'],
                           table=_FISH_TABLE,
                           primary_key="localityweekid")

    data = _extract_fish()
    state['parameter']['fish_select_columns'] = {
        column: column for column in data.columns.tolist()
    }
    state['parameter']['fish_select_columns'].state.pop('timestamp')

    for column in data.columns:
        if data.dtypes[column] == 'object':
            state['parameter']['fish_select_columns'].state.pop(column)


def _download_locality(state):
    _BARENTSWATCH.download(from_year=state['parameter']['from_year'],
                           to_year=state['parameter']['to_year'],
                           locality=state['parameter']['locality']['id'],
                           table=_LOCALITY_TABLE,)

    data = _extract_locality()
    state['parameter']['locality_select_columns'] = {
        column: column for column in data.columns.tolist()
    }
    state['parameter']['locality_select_columns'].state.pop('timestamp')

    for column in data.columns:
        if data.dtypes[column] == 'object':
            state['parameter']['locality_select_columns'].state.pop(column)


def download_weather(state):
    state['flag']['downloading'] = True
    weather_change(state)
    _FROST.download(state['parameter']['from_year'],
                    state['parameter']['to_year'],
                    table=_WEATHER_TABLE)

    data = _extract_weather()
    state['parameter']['weather_select_columns'] = {
        column: column for column in data.columns.tolist()
    }
    state['parameter']['weather_select_columns'].state.pop('timestamp')
    state['flag']['downloading'] = False


# %% EXTRACTING DATA


def _extract_fish():
    return _BARENTSWATCH.load(_FISH_TABLE)


def _extract_locality():
    return _BARENTSWATCH.load(_LOCALITY_TABLE)


def _extract_weather():
    return _FROST.load(_WEATHER_TABLE)


# %% EVENT HANDLERS


def download(state):
    state['flag']['downloading'] = True

    if state['parameter']['download'] == 'fish':
        _download_fish(state)
    elif state['parameter']['download'] == 'locality':
        state['parameter']['locality'] = {
            'id': state['selection']['id'],
            'name': state['selection']['name'].upper()
        }
        _download_locality(state)
    elif state['parameter']['download'] == 'weather':
        download_weather(state)
    else:
        pass

    state['flag']['downloading'] = False
    update_data(state)


def update_data(state):
    state['station_coordinates'] = _FROST.station_coordinates

    data_fish = False
    try:
        data = _extract_fish()
        state['data']['fish'] = data
        state['parameter']['fish_select_columns'] = {
            column: column for column in data.columns.tolist()
        }
        state['parameter']['fish_select_columns'].state.pop('timestamp')
        for column in data.columns:
            if data.dtypes[column] == 'object':
                state['parameter']['fish_select_columns'].state.pop(column)
        data_fish = True
    except AnalysisException:
        logger.warning('No fish data.')

    data_locality = False
    try:
        data = _extract_locality()
        state['data']['locality'] = data
        state['parameter']['locality_select_columns'] = {
            column: column for column in data.columns.tolist()
        }
        state['parameter']['locality_select_columns'].state.pop('timestamp')
        for column in data.columns:
            if data.dtypes[column] == 'object':
                state['parameter']['locality_select_columns'].state.pop(column)
        data_locality = True
        state['flag']['no_data_locality'] = False
    except AnalysisException:
        state['flag']['no_data_locality'] = True

    try:
        data = _extract_weather()
        state['data']['weather'] = data
        state['parameter']['weather_select_columns'] = {
            column: column for column in data.columns.tolist()
        }
        state['parameter']['weather_select_columns'].state.pop('timestamp')
        state['flag']['no_data_weather'] = False
    except AnalysisException:
        state['flag']['no_data_weather'] = True

    if data_fish and data_locality:
        equal = state['data']['fish'][state['data']['fish']['localityno']
                                      == state['data']['locality']['localityno'].values[0]]
        state['parameter']['lon'] = float(equal['lon'].values[0])
        state['parameter']['lat'] = float(equal['lat'].values[0])
        state['parameter']['locality'] = {
            'id': int(equal['localityno'].values[0]),
            'name': str(equal['name'].values[0]).upper()
        }
        state['selection'] = {
            'id': int(equal['localityno'].values[0]),
            'name': str(equal['name'].values[0])
        }
        weather_change(state)
    elif data_fish and not data_locality:
        state['selection'] = state['parameter']['locality'].state


def weather_change(state):
    global _FROST

    _FROST = Frost(_FROST_ID,
                   lon=state['parameter']['lon'], lat=state['parameter']['lat'],
                   count=int(state['parameter']['count']))

    state['station_coordinates'] = _FROST.station_coordinates


def download_change(state):
    state['flag']['locality'] = state['parameter']['download'] == 'locality'
    state['flag']['weather'] = state['parameter']['download'] == 'weather'


_BARENTSWATCH = BarentsWatch(_FISH_ID, _FISH_SECRET)
_FROST = Frost(_FROST_ID, _LON, _LAT, count=5)
