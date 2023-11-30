"""Plotting functions for the website."""

import numpy as np
import pandas as pd
import plotly.express as px
from logging import getLogger
import plotly.graph_objs as go
from scipy.stats import gaussian_kde
from statsmodels.tsa.statespace.sarimax import SARIMAX

from .fetching import download_change, download, update_data


_EMPTY_PLOT = go.Figure()
logger = getLogger(__name__)


# %% PLACEHOLDER PLOT


def empty_plot(state):
    """
    Updates the layout of a global empty plot with state-defined colors.

    Parameters
    ----------
    state : dict
        A dictionary containing the state of the application, specifically 'colours' for background settings.
    """
    logger.debug('Updating empty plot layout')
    global _EMPTY_PLOT

    _EMPTY_PLOT.update_layout(
        paper_bgcolor=state['colours']['background'],
        plot_bgcolor=state['colours']['background'],
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False)
    )
    logger.debug('Empty plot layout updated')


# %% FISH PLOTS


def plot_pd(state):
    """
    Creates a plot for pancreas disease (PD) proportion in fish health data.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data and parameters for the plot.
    """
    logger.info('Creating PD plot')

    try:
        fish_health = state['data']['fish'].copy()
    except AttributeError:
        logger.warning('No fish data found, downloading...')
        state['parameter']['download'] = 'fish'
        download(state)
        update_data(state)
        logger.info('Trying to create PD plot again')
        plot_pd(state)

    fish_health['timestamp'] = pd.to_datetime(fish_health['timestamp'])

    fish_health['haspd_int'] = fish_health['haspd'].astype(int)
    fish_health = fish_health.groupby("timestamp").agg(
        haspd_int=("haspd_int", "sum"), count=("localityweekid", "count")
    ).reset_index()

    fish_health.set_index('timestamp', inplace=True)
    fish_health.sort_index(inplace=True)

    fish_health['prop'] = fish_health['haspd_int'] / fish_health['count']

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=fish_health.index, y=fish_health['prop'],
                             mode='lines', name='Proportion',
                             line=dict(color=state['colours']['black'], width=3),
                             # fill='tozeroy',
                             # fillcolor=state['colours']['green']
                             ))

    fig.update_layout(title='Proportion of localities that have reported Pancreas Disease',
                      xaxis_title='Week',
                      yaxis_title='Proportion',
                      yaxis=dict(range=[-0.01, 0.11],
                                 gridcolor=state['colours']['white'],
                                 zerolinecolor=state['colours']['background']),
                      xaxis=dict(gridcolor=state['colours']['background']),
                      paper_bgcolor=state['colours']['background'],
                      plot_bgcolor=state['colours']['background']
                      )

    state['plot']['pd'] = fig

    logger.info('PD plot created')


# %% LOCALITY PLOTS


def plot_lice(state):
    """
    Creates a plot for lice data based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data, parameters, and flags for the plot.
    """
    logger.info('Creating lice plot')
    state['flag']['threshold'] = False

    try:
        fish_lice = state['data']['locality'].copy()
    except AttributeError:
        logger.warning('No locality data found. Setting flag and placeholder plot.')
        state['flag']['no_data_locality'] = True
        state['plot']['lice'] = _EMPTY_PLOT
        return

    fish_lice['timestamp'] = pd.to_datetime(fish_lice['timestamp'])
    fish_lice.set_index('timestamp', inplace=True)
    fish_lice.sort_index(inplace=True)

    # PLOT SETTINGS
    # --------------------------------------------------

    fig = go.Figure()

    fig.update_layout(
        title=f'Lice data for locality {state["parameter"]["locality"]["name"]}',
        xaxis_title='Week',
        yaxis=dict(title='avgAdultFemaleLice & avgStationaryLice',
                   gridcolor=state['colours']['white'],
                   zerolinecolor=state['colours']['background']),
        xaxis=dict(gridcolor=state['colours']['background'],
                   zerolinecolor=state['colours']['background']),
        yaxis2=dict(title='avgMobileLice', overlaying='y', side='right',
                    gridcolor=state['colours']['white'],
                    zerolinecolor=state['colours']['background']),
        paper_bgcolor=state['colours']['background'],
        plot_bgcolor=state['colours']['background']
    )

    # PLOTTING DATA
    # --------------------------------------------------

    above = state['colours']['red']
    threshold = state['parameter']['lice_threshold']

    def _trace(column, below, axis=None):

        fig.add_trace(go.Scatter(
            x=fish_lice.index,
            y=fish_lice[column],
            mode='lines',
            line=dict(color=below, width=1.5),
            yaxis=axis,
            name=column,
            showlegend=True
        ))

        for idx, value in enumerate(fish_lice[column]):
            if value >= threshold:
                logger.debug(f'Found value above threshold: {value}')
                fig.add_trace(
                    go.Scatter(
                        x=[fish_lice.index[idx]],
                        y=[value],
                        mode='markers',
                        marker=dict(symbol='star', size=10, color=above),
                        yaxis=axis,
                        showlegend=False
                    )
                )

        if fish_lice[column].gt(threshold).any() and not state['flag']['threshold']:
            logger.debug('Found values above threshold – setting flag.')
            state['flag']['threshold'] = True

    _trace('avgadultfemalelice', state['colours']['black'])
    _trace('avgstationarylice', state['colours']['pink'])
    _trace('avgmobilelice', state['colours']['brown'], 'y2')

    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='markers',
        marker=dict(symbol='star', size=10, color=above),
        name='Above threshold'
    ))

    state['plot']['lice'] = fig


# %% NORWAY PLOTS


def _norway(state):
    """
    Prepares and plots data on a map of Norway, highlighting weather stations and fish breeding facilities.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data and parameters for the map plot.
    """
    logger.info('Creating Norway map plot')

    stations = pd.DataFrame(state['station_coordinates'].state.items(),
                            columns=['Name', 'Coordinates'])
    stations[['Lon', 'Lat']] = pd.DataFrame(stations['Coordinates'].tolist(),
                                            index=stations.index)
    stations['Type'] = 'Weather station'
    stations.drop('Coordinates', axis=1, inplace=True)

    try:
        fish = state['data']['fish'].copy()
    except AttributeError:
        logger.warning('No fish data found when plotting Norway, downloading...')
        state['parameter']['download'] = 'fish'
        download(state)
        update_data(state)
        fish = state['data']['fish'].copy()

    fish = fish.groupby('name').agg(
        {'lat': 'first', 'lon': 'first', 'localityno': 'first'}
    ).reset_index()
    fishies = dict(zip(fish['name'], zip(fish['lat'],
                                         fish['lon'],
                                         fish['localityno'])))
    fishies = pd.DataFrame(fishies.items(), columns=['Name', 'Values'])
    fishies[['Lat', 'Lon', 'Locality']] = pd.DataFrame(fishies['Values'].tolist(),
                                                       index=fishies.index)
    fishies['Type'] = 'Fish breeding facility'
    fishies.drop('Values', axis=1, inplace=True)

    data = pd.concat([stations, fishies], ignore_index=False)

    fig = px.scatter_mapbox(
        data,
        lat="Lat",
        lon="Lon",
        hover_name="Name",
        hover_data=["Type", "Locality"],
        color="Type",
        color_discrete_map=state['colours']['mapping'].state,
        zoom=9,
        height=600,
        width=1100,
    )
    for trace in fig.data:
        trace.showlegend = False

    fig.update_layout(mapbox_style="open-street-map",
                      mapbox=dict(center=dict(lat=state['parameter']['lat'],
                                              lon=state['parameter']['lon'])),
                      paper_bgcolor=state['colours']['background'],
                      plot_bgcolor=state['colours']['background'],
                      showlegend=False,
                      )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    data['Name'] = data['Name'].astype(str)
    data['Type'] = data['Type'].astype(str)
    data['Locality'] = data['Locality'].fillna(0)
    data['Locality'] = data['Locality'].astype(int)

    state['data']['norway'] = data
    state['plot']['norway'] = fig

    logger.info('Norway map plot created')


def plot_norway(state):
    """
    Updates and modifies the Norway map plot based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data and parameters for updating the map plot.
    """
    logger.info('Updating selection in Norway map plot')

    state['parameter']['download'] = 'locality'
    download_change(state)

    fig = state['plot']['norway']

    if not fig:
        logger.warning('No Norway map plot found, creating.')
        _norway(state)
        logger.info('Trying to update Norway map plot again')
        plot_norway(state)
        return

    fig.data = [
        trace for trace in fig.data
        if not (getattr(trace, 'marker', None)
                and trace.marker.size == 15
                and trace.marker.color == state['colours']['mapping']['Selected'])
    ]

    try:
        name = state['selection']['name']
        locality_id = state['selection']['id']
    except TypeError:
        logger.warning('No selection found, using default values.')
        name = state['parameter']['locality']['name']
        locality_id = state['parameter']['locality']['id']

    fig.add_trace(go.Scattermapbox(
        lat=[state['parameter']['lat']],
        lon=[state['parameter']['lon']],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=15, color=state['colours']['mapping']['Selected']
        ),
        hoverinfo='text',
        hovertemplate=(
                name + "<br><br>" +
                "Type: Fish breeding facility<br>" +
                "Lat: " + str(round(state['parameter']['lat'], 5)) + "<br>" +
                "Lon: " + str(round(state['parameter']['lon'], 5)) + "<br>" +
                "Locality: " + str(locality_id)
        ),
        text=name,
        showlegend=False,
    ))

    fig.update_layout(
        mapbox=dict(center=dict(lat=state['parameter']['lat'],
                                lon=state['parameter']['lon'])),
        showlegend=False,
    )

    state['plot']['norway'] = fig

    logger.info('Norway map plot updated')


# %% TEMPORARY PLOTS


def plot_temporary(state):
    """
    Creates a temporary plot based on the current page and selected parameters.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data and parameters for the temporary plot.
    """
    logger.info('Creating temporary plot')

    columns = state['parameter']['column']

    if not columns or columns == [] or not state['parameter']['type']:
        logger.warning('No columns or type found, using placeholder plot.')
        state['plot']['temporary'] = _EMPTY_PLOT
        return

    # DATA PREPARATION
    # --------------------------------------------------

    try:
        data = state['data'][state['parameter']['current_page']].copy()
    except AttributeError:
        logger.warning('No data found, using placeholder plot and setting flag.')
        state['flag'][f'no_{state["parameter"]["current_page"]}'] = True
        state['plot']['temporary'] = _EMPTY_PLOT
        return

    data.replace({'null': 0}, inplace=True)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data.set_index('timestamp', inplace=True)
    data.sort_index(inplace=True)

    if state['parameter']['current_page'] == 'fish':
        logger.debug('Preparing fish data')
        data = data.groupby('timestamp').agg({col: 'mean' if data.dtypes[col]
                                              != 'bool' else 'sum'
                                              for col in columns}).reset_index()

    # PLOTTING
    # --------------------------------------------------

    fig = go.Figure()

    for column in columns:
        if state['parameter']['type'] == 'line':
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[column],
                mode='lines',
                name=column,
                line=dict(width=1.5, color=state['colours']['black'] if len(columns) == 1 else None)
            ))
        elif state['parameter']['type'] == 'bar':
            grouped = data.groupby(column).size().reset_index(name='Count')

            fig.add_trace(go.Bar(
                x=grouped[column],
                y=grouped['Count'],
                name=column,
                marker_color=state['colours']['black'] if len(columns) == 1 else None
            ))
        elif state['parameter']['type'] == 'density':
            try:
                density = gaussian_kde(data[column].dropna())
            except np.linalg.LinAlgError:
                logger.warning('Not enough variance, skipping column.')
                continue

            bars = np.linspace(data[column].min(), data[column].max(), 200)
            densities = density.evaluate(bars)

            fig.add_trace(go.Scatter(
                x=bars,
                y=densities,
                mode='lines',
                name=column,
                line=dict(width=1.5, color=state['colours']['black'] if len(columns) == 1 else None)
            ))

    if state['parameter']['current_page'] == 'weather':
        names = {col.rsplit('_', 1)[0]
                 .replace('sum_', '')
                 .replace('_amount', '')
                 .replace('_p1d', '')
                 .replace('_', ' ')
                 for col in columns}
    else:
        names = {col
                 .replace('sum', '')
                 .replace('amount', '')
                 .replace('p1d', '')
                 .replace('_', ' ')
                 for col in columns}
    title = 'Value' if len(names) > 3 else ', '.join(names)

    fig.update_layout(
        xaxis=dict(gridcolor=state['colours']['background'],
                   zerolinecolor=state['colours']['background']),
        yaxis=dict(gridcolor=state['colours']['white'],
                   zerolinecolor=state['colours']['background']),
        paper_bgcolor=state['colours']['background'],
        plot_bgcolor=state['colours']['background']
    )

    if state['parameter']['type'] == 'line':
        fig.update_layout(
            xaxis_title='Week',
            yaxis_title=title,
        )
    elif state['parameter']['type'] == 'bar':
        if state['parameter']['current_page'] == 'fish':
            fig.update_layout(
                xaxis_title='Aggregated value',
                yaxis_title='Count',
            )
        else:
            fig.update_layout(
                xaxis_title='Value',
                yaxis_title='Count',
            )
    elif state['parameter']['type'] == 'density':
        fig.update_layout(
            xaxis_title='Value of ' + title,
            yaxis_title='Density',
        )

    state['plot']['temporary'] = fig


# %% ARIMAX


def arimax(state):
    """
    Creates an ARIMAX model based on the current state and plots the predictions.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application, containing data and parameters for ARIMAX modeling and plotting.
    """
    logger.info('Creating ARIMAX model')

    state['flag']['arimax'] = False
    state['plot']['arimax'] = _EMPTY_PLOT

    if state['parameter']['arimax']['endogenous'] is None:
        return

    # ENDOGENOUS PREPARATION
    # --------------------------------------------------

    try:
        endogenous = state['data']['locality'].copy()
    except AttributeError:
        logger.warning('No locality data found. Setting flag and placeholder plot.')
        state['flag']['no_data_locality'] = True
        state['plot']['arimax'] = _EMPTY_PLOT
        return

    endogenous['timestamp'] = pd.to_datetime(endogenous['timestamp'], utc=True)
    endogenous.set_index('timestamp', inplace=True)
    endogenous.sort_index(inplace=True)

    endogenous = endogenous[state['parameter']['arimax']['endogenous']]

    # EXOGENOUS PREPARATION
    # --------------------------------------------------

    try:
        exogenous = state['data']['weather'].copy()
        exogenous['timestamp'] = pd.to_datetime(exogenous['timestamp'], utc=True)
        exogenous.set_index('timestamp', inplace=True)
        exogenous.sort_index(inplace=True)

        exogenous = exogenous[state['parameter']['arimax']['exogenous']]
        if not exogenous.empty:
            for column in exogenous.columns:

                if exogenous[column].isna().sum() / len(exogenous) < 0.2:
                    exogenous[column].fillna(exogenous[column].mean(), inplace=True)
                elif len(exogenous[column].unique()) >= 2:
                    pass
                else:
                    exogenous.drop(column, axis=1, inplace=True)
                    continue

                for lag in range(1, int(state['parameter']['arimax']['lags']) + 1):
                    try:
                        exogenous[f'{column}_lag{lag}'] = exogenous[column].shift(lag)
                    except KeyError:
                        continue
            if exogenous.empty:
                logger.warning('No exogenous data found. Not using exogenous data.')
                exogenous = None
            else:
                exogenous.bfill(inplace=True)
                exogenous = exogenous.reindex(endogenous.index, method='ffill')
                exogenous = exogenous.loc[exogenous.index.isin(endogenous.index)]
                endogenous = endogenous.loc[endogenous.index.isin(exogenous.index)]

                if exogenous.empty or endogenous.empty:
                    logger.error('Endogenous and exogenous data is '
                                 'incompatible (no common indices)!')
                    return
        else:
            logger.warning('No exogenous data found. Not using exogenous data.')
            exogenous = None
    except AttributeError:
        logger.warning('No weather data found. Setting flag and not using exogenous data.')
        state['flag']['no_data_weather'] = True
        exogenous = None

    # MODELLING
    # --------------------------------------------------

    horizon = int(state['parameter']['arimax']['horizon'])

    train_endogenous = endogenous.iloc[:-horizon]
    test_endogenous = endogenous.iloc[-horizon:]

    if exogenous is None:
        train_exogenous = None
        test_exogenous = None
    else:
        train_exogenous = exogenous.iloc[:-horizon]
        test_exogenous = exogenous.iloc[-horizon:]

    p = state['parameter']['arimax']['hyperparameters']['p']
    d = state['parameter']['arimax']['hyperparameters']['d']
    q = state['parameter']['arimax']['hyperparameters']['q']

    full = SARIMAX(train_endogenous, exog=train_exogenous,
                   order=(p, d, q), seasonal_order=(0, 0, 0, 0),
                   enforce_stationarity=False, enforce_invertibility=False)
    results = full.fit(disp=False)

    prediction = results.get_forecast(steps=len(test_endogenous),
                                      exog=test_exogenous).predicted_mean

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=endogenous.index, y=endogenous,
                             mode='lines', name='Actual',
                             line=dict(color=state['colours']['brown'],
                                       width=1),
                             ))
    fig.add_trace(go.Scatter(x=test_endogenous.index, y=prediction,
                             mode='lines', name='Prediction',
                             line=dict(color=state['colours']['black'],
                                       width=3)
                             ))

    fig.update_layout(
        title='ARIMAX prediction',
        xaxis_title='Week',
        yaxis_title=state['parameter']['arimax']['endogenous'],
        yaxis=dict(gridcolor=state['colours']['white'],
                   zerolinecolor=state['colours']['background']),
        xaxis=dict(gridcolor=state['colours']['background'],
                   zerolinecolor=state['colours']['background'],
                   range=[endogenous.index[-horizon-3], endogenous.index[-1]]),
        paper_bgcolor=state['colours']['background'],
        plot_bgcolor=state['colours']['background']
        )

    state['arimax_results'] = pd.DataFrame(
        {
            'Parameter value': results.params,
            'p-value': results.pvalues
        }
    )
    state['plot']['arimax'] = fig
    state['flag']['arimax'] = True

    logger.info('ARIMAX model created')


# %% UPDATERS


def initialise_plots(state):
    """
    Initializes all plots based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Initialising plots')

    _norway(state)
    plot_norway(state)

    empty_plot(state)
    plot_temporary(state)

    plot_pd(state)
    plot_lice(state)

    arimax(state)

    logger.info('Plots initialised')


def set_page_fish(state):
    """
    Sets the current page to 'fish' in the state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Setting page to fish')
    state['parameter']['current_page'] = 'fish'


def update_plot_fish(state):
    """
    Updates the fish plot based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Updating fish plot')
    state['parameter']['current_page'] = 'fish'
    state['parameter']['type'] = None
    state['parameter']['column'] = []
    state['plot']['temporary'] = _EMPTY_PLOT

    plot_pd(state)


def set_page_locality(state):
    """
    Sets the current page to 'locality' in the state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Setting page to locality')
    state['parameter']['current_page'] = 'locality'


def update_plot_locality(state):
    """
    Updates the locality plot based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Updating locality plot')
    state['parameter']['current_page'] = 'locality'
    state['parameter']['type'] = None
    state['parameter']['column'] = []
    state['plot']['temporary'] = _EMPTY_PLOT

    plot_lice(state)


def set_page_weather(state):
    """
    Sets the current page to 'weather' in the state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Setting page to weather')
    state['parameter']['current_page'] = 'weather'


def update_plot_weather(state):
    """
    Updates the weather plot based on the current state.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    """
    logger.info('Updating weather plot')
    state['parameter']['current_page'] = 'weather'
    state['parameter']['type'] = None
    state['parameter']['column'] = []
    state['plot']['temporary'] = _EMPTY_PLOT


# %% HANDLERS


def map_click(state, payload):
    """
    Handles click events on the map and updates the state accordingly.

    Parameters
    ----------
    state : dict
        A dictionary representing the state of the application.
    payload : list
        A list containing payload data from the map click event.
    """
    logger.info('Handling map click event')
    logger.debug(f'Payload: {payload}')

    selection = state['data']['norway'].loc[payload[0]['pointNumber']]

    if len(selection) != 5:
        return

    state['parameter']['lon'] = selection['Lon']
    state['parameter']['lat'] = selection['Lat']
    state['selection'] = {'id': int(selection['Locality']), 'name': selection['Name']}

    plot_norway(state)

    logger.info('Map click event handled')
