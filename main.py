"""State management and initialisation of the website."""

import warnings
import streamsync as ss

from functions.fetching import update_data
from functions.plots import initialise_plots


warnings.simplefilter(action='ignore', category=FutureWarning)

state = ss.init_state({
    'session': None,
    'colours': {
        'pink': '#c37892',
        'brown': '#8a795d',
        'purple': '#746cc0',
        'red': 'red',
        'background': '#AAD3DF',
        'green': '#BCDAB1',
        'black': '#143439',
        'white': '#F3EFE8',
        'mapping': {
            'Fish breeding facility': '#ff6d3a',
            'Weather station': '#8a795d',
            'Selected': 'black'
        }
    },

    'parameter': {
        'from_year': 2021,
        'to_year': 2021,

        'locality': {'id': 11318, 'name': 'HALLVARDØY'},
        'lon': 15.317333221435547,
        'lat': 68.37496948242188,
        'count': 5,

        'download': 'fish',
        'current_page': 'fish',

        'lice_threshold': 3.0,

        'fish_select_columns': {},
        'locality_select_columns': {},
        'weather_select_columns': {},

        'column': [],
        'type': None,

        'arimax': {
            'endogenous': 'avgadultfemalelice',
            'exogenous': [],
            'lags': 6,
            'horizon': 8,
            'hyperparameters': {
                'p': 11,
                'd': 4,
                'q': 7,
            },
        }
    },
    'arimax_results': {},

    'data': {
        'fish': None,
        'locality': None,
        'weather': None,
        'norway': None,
    },

    'plot': {
        'norway': None,
        'pd': None,
        'lice': None,
        'temporary': None,
        'arimax': None
    },

    'flag': {
        'no_data_weather': False,
        'no_data_locality': False,

        'downloading': False,
        'locality': False,
        'weather': False,
        'threshold': False,
        'arimax': False,
    },
})

update_data(state)
initialise_plots(state)
