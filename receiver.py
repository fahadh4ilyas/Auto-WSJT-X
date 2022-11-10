import re, socket, select, wsjtx, struct, requests, typing, time

from datetime import datetime, timedelta

from pyhamtools.locator import latlong_to_locator
from pyhamtools.frequency import freq_to_band

from states import States
from config import *
from adif_parser import main as adif_parser, db, done_coll, call_info, call_info2, country_to_dxcc
import logging
from logging import handlers

IP_LOCK = []

DXCC_EXCEPTION = [country_to_dxcc.get(i,0) for i in DXCC_EXCEPTION]

callsign_exc = []
if CALLSIGN_EXCEPTION:
    try:
        with open(CALLSIGN_EXCEPTION) as f:
            callsign_exc = f.read().splitlines()
    except:
        pass

receiver_exc = []
if RECEIVER_EXCEPTION:
    try:
        with open(RECEIVER_EXCEPTION) as f:
            receiver_exc = f.read().splitlines()
    except:
        pass

with open(DXCC_PRIORITY) as f:
    priority_country_list = f.read().splitlines()
    length_priority_country_list = len(priority_country_list)
    priority_country = dict(
        [
            (
                d,
                0.5-i/(2*length_priority_country_list+1)
            ) for i,d in enumerate(priority_country_list, start=1)
        ]
    )

LOCAL_STATES = {
    'my_callsign': '',
    'states_completed': False,
    'current_callsign': ''
}

NEXT_TRANSMIT = {
    True: {
        'GRID': 'SNR',
        'SNR': 'RSNR',
        'RSNR': 'R73',
        'R73': 'R73'
    }
}

STATES_LIST: typing.Dict[str, States] = {
    '': States(REDIS_HOST, REDIS_PORT, MULTICAST)
}

if MULTICAST:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
else:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_wsjt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_wsjt.setblocking(False) # Set socket to non-blocking mode
sock_wsjt.setblocking(0)
bind_addr = socket.gethostbyname(WSJTX_IP)

grid_coll = db.grid
call_coll = db.calls
message_coll = db.message

def filter_cq(data: dict, states: States) -> bool:

    if data['SNR'] < states.min_db:
        return False

    if DXCC_EXCEPTION and 'dxcc' in data and data['dxcc'] in DXCC_EXCEPTION:
        return False

    if data.get('extra', None):
        if (data['extra'] == 'DX' and data.get('country', '') == 'Indonesia') or data['extra'] != 'OC':
            return False
        
    if 'grid' in data and states.new_grid and not done_coll.find_one(
        {
            'grid': data['grid'],
            'band': data['band'],
            'mode': data['mode']
        }
    ):
        return True
    
    if data['isNewDXCC'] and states.new_dxcc:
        return True
    
    if data['isNewCallsign']:
        return True
    
    return False

def parsing_message(message: str) -> dict:
    message_type = ''
    matching = None
    for types, c in wsjtx.call_types.items():
        matching = c.match(message)
        if matching:
            message_type = types
            break
    
    if matching:
        return {**matching.groupdict(), 'type': message_type}

    return {}

def get_location_data(callsign: str, latest_data: dict = {}) -> dict:

    if latest_data and all([i in latest_data for i in ['country', 'dxcc', 'continent']]):
        return {
            k: latest_data[k] for k in ['country', 'dxcc', 'continent']
        }
    try:
        location_data = call_info.get_all(callsign)
        location_data['dxcc'] = country_to_dxcc.get(location_data['country'], 0)
    except:
        location_data = {}
    
    return location_data

def get_grid_data(
    callsign: str,
    grid: typing.Optional[str] = None,
    location_data: dict = {},
    latest_data: dict = {}
    ) -> dict:

    data = {'grid': None}
    if grid:
        data['grid'] = grid
    elif latest_data and 'grid' in latest_data:
        data['grid'] = latest_data['grid']
    else:
        current_grid = (grid_coll.find_one({'callsign': callsign}) or {}).get('grid', None)
        if not current_grid and 'latitude' in location_data:
            current_grid = latlong_to_locator(location_data['latitude'], location_data['longitude'])[:4]
        if current_grid:
            data['grid'] = current_grid
    
    return data

def completing_data(data: dict, additional_data: dict, now: float = None, latest_data: dict = {}) -> dict:

    location_data = get_location_data(data['prefixed_callsign'], latest_data)
    if location_data:
        data.update({
            k: location_data[k] for k in ['country', 'dxcc', 'continent']
        })
    
    grid_data = get_grid_data(data['callsign'], data.get('grid', None), location_data)
    if grid_data:
        data.update(grid_data)
    if data['grid'] is None:
        data.pop('grid')

    data.update(additional_data)

    data['expired'] = False
    data['tried'] = False
    data['isReemerging'] = False
    data['isSpam'] = False
    data['isEven'] = (0 <= (data['Time']/1000)%TIMING[data['mode']]['full'] < TIMING[data['mode']]['half'])
    data['skipGrid'] = True
    data['isNewCallsign'] = not done_coll.find_one(
        {
            'callsign': data['callsign'],
            'band': data['band'],
            'mode': data['mode']
        }
    )
    data['isNewDXCC'] = latest_data.get('isNewDXCC', not done_coll.find_one(
        {
            'dxcc': data.get('dxcc', 0),
            'band': data['band'],
            'mode': data['mode']
        }
    ))
    data['timestamp'] = now or datetime.now().timestamp()
    
    return data

def get_state_data(callsign: str) -> dict:

    data = {}
    if call_info2:
        try:
            state_data = call_info2.get_all(callsign)
            if 'state' in state_data:
                data['state'] = state_data['state']
            if 'county' in state_data:
                data['county'] = state_data['county']
        except:
            pass
    
    return data

def get_transmit_data_type(data: dict) -> str:
    global NEXT_TRANSMIT, LOCAL_STATES

    return NEXT_TRANSMIT.get(
        data.get('to', None) == LOCAL_STATES['my_callsign'], 
        {}
    ).get(data['type'], 'SNR' if data.get('skipGrid', True) else 'GRID')

def process_wsjt(_data: bytes, ip_from: tuple, states: States):
    global callsign_exc, receiver_exc, LOCAL_STATES

    try:
        packet = wsjtx.ft8_decode(_data)
    except (IOError, NotImplementedError):
        logging.exception('Something not right!')
        return

    if isinstance(packet, wsjtx.WSHeartbeat):

        logging.info(f'IP: {ip_from[0]} | Port: {ip_from[1]}')
        states.change_states(
            closed = False
        )
        if CALLSIGN_EXCEPTION:
            try:
                with open(CALLSIGN_EXCEPTION) as f:
                    callsign_exc = f.read().splitlines()
            except:
                pass
        if RECEIVER_EXCEPTION:
            try:
                with open(RECEIVER_EXCEPTION) as f:
                    receiver_exc = f.read().splitlines()
            except:
                pass
    
    elif isinstance(packet, wsjtx.WSStatus):

        now = datetime.now().timestamp()
        logging.debug(packet)
        LOCAL_STATES['my_callsign'] = packet.DeCall or ''
        states.change_states(
            my_callsign = packet.DeCall or '',
            my_grid = packet.DeGrid or '',
            dx_callsign = packet.DXCall or '',
            dx_grid = packet.DXGrid or '',
            tx_enabled = packet.TXEnabled,
            decoding = packet.Decoding,
            txdf = packet.TXdf,
            rxdf = packet.RXdf,
            tx_even = packet.TxEven
        )

        states_list = states.get_states(
            'band',
            'mode',
            'transmitting'
        )

        latest_band = states_list['band']
        latest_mode = states_list['mode']
        current_band: int = freq_to_band(packet.Frequency//1000)['band']
        current_mode = packet.Mode
        isTransmitting = packet.Transmitting and states_list['transmitting'] != packet.Transmitting
        isDoneTransmitting = not packet.Transmitting and states_list['transmitting'] != packet.Transmitting
        isChangingBand = latest_band != 0 and latest_band != current_band
        isChangingMode = latest_mode != '' and latest_mode != current_mode

        states.transmitting = packet.Transmitting

        if isTransmitting:
            packet_last_tx = packet.LastTxMsg or ''

            logging.info(
                f'[TX] [MODE: {current_mode}] [BAND: {current_band}] '
                f'[FREQUENCY: {states.txdf}] {packet_last_tx}'
            )

            matched = parsing_message(packet_last_tx)
            latest_tx = states.last_tx
            matched_latest = parsing_message(latest_tx)

            isDifferent = latest_tx != packet_last_tx

            states.last_tx = packet_last_tx
            LOCAL_STATES['current_callsign'] = matched.get('current_callsign', '')

            isSameMessage = matched.get('type', None) == matched_latest.get('type', None) and \
                matched.get('to', None) == matched_latest.get('to', None)

            if EXPIRED_TIME:
                call_coll.update_many(
                    {
                        'timestamp': {'$lte': now-EXPIRED_TIME+TIMING[current_mode]['full']}, 
                        'importance': {'$lt': 2}
                    },
                    {'$set': {'expired': True}}
                )
            states.even_frequencies = [MIN_FREQUENCY, MAX_FREQUENCY]
            states.odd_frequencies = [MIN_FREQUENCY, MAX_FREQUENCY]

            if states.transmitter_started:
                if not isSameMessage:
                    states.change_states(
                        tries = 1,
                        inactive_count = 1,
                        transmit_counter = 1
                    )
                else:
                    states_list = states.get_states('tries', 'inactive_count', 'transmit_counter')
                    states.change_states(
                        tries = states_list['tries'] + 1,
                        inactive_count = states_list['inactive_count'] + 1,
                        transmit_counter = states_list['transmit_counter'] + 1
                    )

                result = {}
                if matched.get('type', 'CQ') != 'CQ':
                    result = call_coll.find_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                    ) or {}
                
                states_list = states.get_states(
                    'num_inactive_before_cut',
                    'inactive_count',
                    'tries',
                    'max_tries',
                    'transmit_counter'
                )

                if states_list['tries'] >= result.get('tries', states_list['max_tries']):
                    states.change_states(
                        tries = 0,
                        inactive_count = 0
                    )
                    states_list.update(
                        {
                            'tries': 0,
                            'inactive_count': 0
                        }
                    )
                    if result:
                        logging.warning(
                            f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                            f'[CALLSIGN: {matched["to"]}] Max tried {result["Message"]}'
                        )
                    call_coll.update_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode},
                        {'$set': {'tried': True}}
                    )

                if states_list['num_inactive_before_cut'] and states_list['inactive_count'] > states_list['num_inactive_before_cut']:
                    states.change_states(
                        tries = 0,
                        inactive_count = 0
                    )
                    if result:
                        logging.warning(
                            f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                            f'[CALLSIGN: {matched["to"]}] Max tried after inactive {result["Message"]}'
                        )
                    call_coll.update_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode},
                        {'$set': {'expired': True}}
                    )
                
                if states_list['transmit_counter'] >= 2*states_list['max_tries']:
                    states.change_states(
                        tries = 0,
                        inactive_count = 0,
                        transmit_counter = 0
                    )
                    if result:
                        logging.warning(
                            f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                            f'[CALLSIGN: {matched["to"]}] Looping message {result["Message"]}'
                        )
                    call_coll.update_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode},
                        {'$set': {'tried': True, 'isSpam': True}}
                    )

            else:
                states.change_states(
                    tries = 0,
                    inactive_count = 0,
                    transmit_counter = 0
                )

            if isDifferent and matched.get('type', None) == 'R73':
                qso_data = done_coll.find_one(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                ) or {}
                if not qso_data:
                    logging.info(f'Logging QSO: {matched["to"]} at band {current_band} in mode {current_mode}')
                    states.log_qso()
                if states.transmitter_started and matched['R73'] != '73':
                    current_data = call_coll.find_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                    ) or {}
                else:
                    current_data = call_coll.find_one_and_delete(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                    ) or {}
                    if current_data:
                        logging.warning(
                            f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                            f'[CALLSIGN: {matched["to"]}] Removing {current_data["Message"]}'
                        )
                if not current_data:
                    current_data['callsign'] = matched['to']
                current_data.update({
                    'confirmed': True,
                    'fromScript': True,
                    'timestamp': now,
                    'callsign': matched['to'],
                    'band': current_band,
                    'mode': current_mode
                })
                location_data = get_location_data(current_data['prefixed_callsign'], current_data)
                if 'country' not in current_data  and all([i in location_data for i in ['country', 'dxcc', 'continent']]):
                    current_data.update({
                        k: location_data[k] for k in ['country', 'dxcc', 'continent']
                    })
                grid_data = get_grid_data(
                    current_data['callsign'],
                    current_data.get('grid', None),
                    location_data,
                    current_data
                )
                current_data.update(grid_data)
                if current_data.get('grid', None) is None:
                    current_data.pop('grid')
                if call_info2 and current_data.get('country', None) == 'United States' and 'state' not in current_data:
                    if 'state' in qso_data:
                        current_data.update(
                            {
                                k:qso_data[k] for k in ['state', 'county']
                            }
                        )
                    else:
                        state_data = get_state_data(current_data['callsign'])
                        current_data.update(state_data)
                current_data.pop('_id', None)
                done_coll.update_one(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode},
                    {'$set': current_data},
                    upsert=True
                )
            
            if matched.get('type', None) == 'R73' and not (states.transmitter_started and matched['R73'] != '73'):
                result = call_coll.find_one_and_delete(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                )
                if result:
                    logging.warning(
                        f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                        f'[CALLSIGN: {matched["to"]}] Removing {result["Message"]}'
                    )

        if isDoneTransmitting:
            states_list = states.get_states(
                'num_disable_transmit',
                'enable_transmit_counter',
                'transmitter_started'
            )

            packet_last_tx = packet.LastTxMsg or ''

            matched = parsing_message(packet_last_tx)
            latest_tx = states.last_tx
            matched_latest = parsing_message(latest_tx)

            isDifferent = latest_tx != packet_last_tx

            if isDifferent and matched.get('type', None) == 'R73':
                qso_data = done_coll.find_one(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                ) or {}
                if not qso_data:
                    logging.info(f'Logging QSO: {matched["to"]} at band {current_band} in mode {current_mode}')
                    states.log_qso()
                if states.transmitter_started and matched['R73'] != '73':
                    current_data = call_coll.find_one(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                    ) or {}
                else:
                    current_data = call_coll.find_one_and_delete(
                        {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                    ) or {}
                    if current_data:
                        logging.warning(
                            f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                            f'[CALLSIGN: {matched["to"]}] Removing {current_data["Message"]}'
                        )
                if not current_data:
                    current_data['callsign'] = matched['to']
                current_data.update({
                    'confirmed': True,
                    'fromScript': True,
                    'timestamp': now,
                    'callsign': matched['to'],
                    'band': current_band,
                    'mode': current_mode
                })
                location_data = get_location_data(current_data['prefixed_callsign'], current_data)
                if 'country' not in current_data  and all([i in location_data for i in ['country', 'dxcc', 'continent']]):
                    current_data.update({
                        k: location_data[k] for k in ['country', 'dxcc', 'continent']
                    })
                grid_data = get_grid_data(
                    current_data['callsign'],
                    current_data.get('grid', None),
                    location_data,
                    current_data
                )
                current_data.update(grid_data)
                if current_data.get('grid', None) is None:
                    current_data.pop('grid')
                if call_info2 and current_data.get('country', None) == 'United States' and 'state' not in current_data:
                    if 'state' in qso_data:
                        current_data.update(
                            {
                                k:qso_data[k] for k in ['state', 'county']
                            }
                        )
                    else:
                        state_data = get_state_data(current_data['callsign'])
                        current_data.update(state_data)
                current_data.pop('_id', None)
                done_coll.update_one(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode},
                    {'$set': current_data},
                    upsert=True
                )
            
            if matched.get('type', None) == 'R73' and not (states.transmitter_started and matched['R73'] != '73'):
                result = call_coll.find_one_and_delete(
                    {'callsign': matched['to'], 'band': current_band, 'mode': current_mode}
                )
                if result:
                    logging.warning(
                        f'[DB] [MODE: {current_mode}] [BAND: {current_band}] '
                        f'[CALLSIGN: {matched["to"]}] Removing {result["Message"]}'
                    )
            
            if states_list['num_disable_transmit']:
                if states_list['transmitter_started']:
                    value = (states_list['enable_transmit_counter'] + 1) % states_list['num_disable_transmit']
                    time.sleep(0.5)
                    if value == 0:
                        states.disable_transmit()
                    states.enable_monitoring()
                else:
                    value = 0
                states.enable_transmit_counter = value

        if isChangingBand:
            logging.warning('Changing band by user!')
            logging.warning(f'[DB] [MODE: {latest_mode}] [BAND: {latest_band}] Removing all message!')
            call_coll.delete_many({'band': latest_band, 'mode': latest_mode})
            message_coll.delete_many({'band': latest_band, 'mode': latest_mode})
        
        if isChangingMode:
            logging.warning('Changing mode by user!')
            logging.warning(f'[DB] [MODE: {latest_mode}] Removing all message!')
            call_coll.delete_many({'mode': latest_mode})
            message_coll.delete_many({'mode': latest_mode})

        states.change_states(
            band = current_band,
            mode = current_mode
        )

        LOCAL_STATES['states_completed'] = True

    elif isinstance(packet, wsjtx.WSDecode):

        if not LOCAL_STATES['states_completed']:
            return

        now = datetime.now().timestamp()

        states_list = states.get_states(
            'band',
            'mode',
            'num_inactive_before_cut',
            'num_tries_call_busy'
        )

        if MIN_FREQUENCY <= packet.DeltaFrequency <= MAX_FREQUENCY:
            delta_time = packet.Time/1000
            if 0 <= delta_time%TIMING[states_list['mode']]['full'] < TIMING[states_list['mode']]['half']:
                states.add_even_frequency(packet.DeltaFrequency)
            else:
                states.add_odd_frequency(packet.DeltaFrequency)
        
        logging.info(
            f'[RX] [MODE: {states_list["mode"]}] [BAND: {states_list["band"]}] '
            f'[FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {packet.Message}'
        )

        data = packet.as_dict()
        data.update(parsing_message(packet.Message))

        if 'type' not in data:
            logging.warning('Cannot parsing the message!')
            return

        if data['callsign'] in callsign_exc:
            logging.warning('The Callsign is blacklisted in callsign exception!')
            return

        latest_data = call_coll.find_one_and_delete(
            {'callsign': data['callsign'], 'band': states_list['band'], 'mode': states_list['mode']}
        ) or {}
        latest_data.pop('_id', None)
        if latest_data:
            logging.warning(
                f'[DB] [MODE: {states_list["mode"]}] [BAND: {states_list["band"]}] '
                f'[CALLSIGN: {latest_data["callsign"]}] Removing {latest_data["Message"]}'
            )

        additional_data = {
            'band': states_list['band'],
            'mode': states_list['mode']
        }
        completing_data(
            data,
            additional_data,
            now,
            latest_data or message_coll.find_one(
                {'callsign': data['callsign'], 'band': states_list['band'], 'mode': states_list['mode']}
            ) or {}
        )

        message_coll.update_one(
            {'callsign': data['callsign'], 'band': states_list['band'], 'mode': states_list['mode']},
            {'$set': data},
            upsert=True
        )

        if 'country' not in data:
            logging.warning('The Callsign\'s country is not found')
            return

        if states_list['num_inactive_before_cut'] and data['callsign'] == LOCAL_STATES['current_callsign']:
            states.inactive_count = 0

        if data['type'] == 'CQ':

            if data.get('grid', None):
                grid_coll.update_one({'callsign': data['callsign']}, {'$set': {
                    'callsign': data['callsign'],
                    'grid': data['grid']
                }}, upsert=True)

            if latest_data and latest_data.get('to', None) == LOCAL_STATES['my_callsign']:
                if latest_data.get('R73', None) != '73':
                    logging.warning('Already CQ-ing even though still talking with me!')
                    if not (latest_data['tried'] and latest_data['isReemerging']):
                        if latest_data['tried']:
                            latest_data['expired'] = False
                            latest_data['tried'] = False
                            latest_data['timestamp'] = data['timestamp']
                            latest_data['isReemerging'] = True
                        logging.info(
                            f'[DB] [MODE: {latest_data["mode"]}] [BAND: {latest_data["band"]}] '
                            f'[CALLSIGN: {latest_data["callsign"]}] Adding back {latest_data["Message"]}'
                        )
                        call_coll.update_one(
                            {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                            {'$set': latest_data},
                            upsert=True
                        )
                        return

            if not data['isNewCallsign']:
                logging.warning('The Callsign is already blacklisted!')
                return

            if not filter_cq(data, states):
                logging.warning('The Callsign is not following criteria!')
                return

            logging.info(
                f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
            )
            data['importance'] = 1 + priority_country.get(data['country'], 0)
            call_coll.update_one(
                {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                {'$set': data},
                upsert=True
            )

        elif data['type'] == 'R73':

            if data['to'] == LOCAL_STATES['my_callsign']:

                if data['R73'] == '73':
                    return

                else:
                    
                    logging.info(
                        f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                        f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                    )
                    data['importance'] = 4 + priority_country.get(data['country'], 0)
                    if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                        data['isSpam'] = latest_data.get('isSpam', False)
                    call_coll.update_one(
                        {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                        {'$set': data},
                        upsert=True
                    )

            else:

                if latest_data and latest_data.get('to', None) == LOCAL_STATES['my_callsign']:
                    if latest_data.get('R73', None) != '73':
                        logging.warning('Sending 73 to other callsign even though still talking with me!')
                        if not (latest_data['tried'] and latest_data['isReemerging']):
                            if latest_data['tried']:
                                latest_data['expired'] = False
                                latest_data['tried'] = False
                                latest_data['timestamp'] = data['timestamp']
                                latest_data['isReemerging'] = True
                            logging.info(
                                f'[DB] [MODE: {latest_data["mode"]}] [BAND: {latest_data["band"]}] '
                                f'[CALLSIGN: {latest_data["callsign"]}] Adding back {latest_data["Message"]}'
                            )
                            call_coll.update_one(
                                {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                                {'$set': latest_data},
                                upsert=True
                            )
                            return
                
                if not data['isNewCallsign']:
                    logging.warning('The Callsign is already blacklisted!')
                    return

                if not filter_cq(data, states):
                    logging.warning('The Callsign is not following criteria!')
                    return

                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 1 + priority_country.get(data['country'], 0)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

        elif data['type'] == 'GRID':

            if data.get('grid', None):
                grid_coll.update_one({'callsign': data['callsign']}, {'$set': {
                    'callsign': data['callsign'],
                    'grid': data['grid']
                }}, upsert=True
            )

            if data['to'] == LOCAL_STATES['my_callsign']:

                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 1 + priority_country.get(data['country'], 0)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

            else:

                if latest_data and latest_data.get('to', None) == LOCAL_STATES['my_callsign']:
                    if latest_data.get('R73', None) != '73':
                        logging.warning('Sending Grid to other callsign even though still talking with me!')
                        if not (latest_data['tried'] and latest_data['isReemerging']):
                            if latest_data['tried']:
                                latest_data['expired'] = False
                                latest_data['tried'] = False
                                latest_data['timestamp'] = data['timestamp']
                                latest_data['isReemerging'] = True
                            logging.info(
                                f'[DB] [MODE: {latest_data["mode"]}] [BAND: {latest_data["band"]}] '
                                f'[CALLSIGN: {latest_data["callsign"]}] Adding back {latest_data["Message"]}'
                            )
                            call_coll.update_one(
                                {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                                {'$set': latest_data},
                                upsert=True
                            )
                            return

                if not states_list['num_tries_call_busy']:
                    return
                
                if data['to'] in receiver_exc:
                    logging.warning('The Callsign is calling someone that is blacklisted!')
                    return

                if not data['isNewCallsign']:
                    logging.warning('The Callsign is already blacklisted!')
                    return

                if not filter_cq(data, states):
                    logging.warning('The Callsign is not following criteria!')
                    return

                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 1 + priority_country.get(data['country'], 0)
                data['tries'] = states_list['num_tries_call_busy']
                data['tried'] = latest_data.get('tried', False)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

        elif data['type'] == 'SNR':

            if data['to'] == LOCAL_STATES['my_callsign']:
                
                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 2 + priority_country.get(data['country'], 0)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

            else:
                
                if latest_data and latest_data.get('to', None) == LOCAL_STATES['my_callsign']:
                    if latest_data.get('R73', None) != '73':
                        logging.warning('Sending signal to other callsign even though still talking with me!')
                        if not (latest_data['tried'] and latest_data['isReemerging']):
                            if latest_data['tried']:
                                latest_data['expired'] = False
                                latest_data['tried'] = False
                                latest_data['timestamp'] = data['timestamp']
                                latest_data['isReemerging'] = True
                            logging.info(
                                f'[DB] [MODE: {latest_data["mode"]}] [BAND: {latest_data["band"]}] '
                                f'[CALLSIGN: {latest_data["callsign"]}] Adding back {latest_data["Message"]}'
                            )
                            call_coll.update_one(
                                {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                                {'$set': latest_data},
                                upsert=True
                            )
                            return

                if not states_list['num_tries_call_busy']:
                    return

                if data['to'] in receiver_exc:
                    logging.warning('The Callsign is calling someone that is blacklisted!')
                    return

                if not data['isNewCallsign']:
                    logging.warning('The Callsign is already blacklisted!')
                    return

                if not filter_cq(data, states):
                    logging.warning('The Callsign is not following criteria!')
                    return

                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 1 + priority_country.get(data['country'], 0)
                data['tries'] = states_list['num_tries_call_busy']
                data['tried'] = latest_data.get('tried', False)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

        elif data['type'] == 'RSNR':

            if data['to'] == LOCAL_STATES['my_callsign']:
                
                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 3 + priority_country.get(data['country'], 0)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

            else:
                
                if latest_data.get('to', None) == LOCAL_STATES['my_callsign']:
                    if latest_data.get('R73', None) != '73':
                        logging.warning('Replying signal to other callsign even though still talking with me!')
                        if not (latest_data['tried'] and latest_data['isReemerging']):
                            if latest_data['tried']:
                                latest_data['expired'] = False
                                latest_data['tried'] = False
                                latest_data['timestamp'] = data['timestamp']
                                latest_data['isReemerging'] = True
                            logging.info(
                                f'[DB] [MODE: {latest_data["mode"]}] [BAND: {latest_data["band"]}] '
                                f'[CALLSIGN: {latest_data["callsign"]}] Adding back {latest_data["Message"]}'
                            )
                            call_coll.update_one(
                                {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                                {'$set': latest_data},
                                upsert=True
                            )
                            return

                if not states_list['num_tries_call_busy']:
                    return

                if data['to'] in receiver_exc:
                    logging.warning('The Callsign is calling someone that is blacklisted!')
                    return

                if not data['isNewCallsign']:
                    logging.warning('The Callsign is already blacklisted!')
                    return

                if not filter_cq(data, states):
                    logging.warning('The Callsign is not following criteria!')
                    return

                logging.info(
                    f'[DB] [MODE: {data["mode"]}] [BAND: {data["band"]}] '
                    f'[CALLSIGN: {data["callsign"]}] Adding {data["Message"]}'
                )
                data['importance'] = 1 + priority_country.get(data['country'], 0)
                data['tries'] = states_list['num_tries_call_busy']
                data['tried'] = latest_data.get('tried', False)
                if latest_data and get_transmit_data_type(latest_data) == get_transmit_data_type(data):
                    data['isSpam'] = latest_data.get('isSpam', False)
                call_coll.update_one(
                    {'callsign': data['callsign'], 'band': data['band'], 'mode': data['mode']},
                    {'$set': data},
                    upsert=True
                )

    elif isinstance(packet, wsjtx.WSADIF):
        logging.info(f'LOGGED ADIF: {packet.ADIF}')

    elif isinstance(packet, wsjtx.WSClose):
        logging.warning(packet)
        states.closed = True
        raise KeyboardInterrupt('WSJT-X Closed!')

    else:
        logging.debug(packet)

def init(sock: socket.socket, states: States):

    logging.info('Initializing...')
    states.r.flushdb()
    states.new_grid = NEW_GRID
    states.new_dxcc = NEW_DXCC
    states.min_db = MIN_DB
    states.num_inactive_before_cut = NUM_INACTIVE_BEFORE_CUT
    states.num_tries_call_busy = NUM_TRIES_CALL_BUSY
    states.num_disable_transmit = NUM_DISABLE_TRANSMIT
    states.max_tries = MAX_TRIES

    if QRZ_API_KEY:
        logging.info('Checking QRZ Logbook...')
        if WORK_ON_UNCONFIRMED_QSO:
            logging.info('Removing unconfirmed log from blacklist...')
            done_coll.delete_many({'$or': [{'confirmed': False}, {'fromScript': True}]})
        if NUM_DAYS_LOG:
            now = datetime.now()
            previous = now - timedelta(days=NUM_DAYS_LOG)
            now_str = now.strftime('%Y-%m-%d')
            previous_str = previous.strftime('%Y-%m-%d')
            logging.info(f'Getting log from {previous_str} to {now_str}...')
            res = requests.post(
                'https://logbook.qrz.com/api',
                data=f'KEY={QRZ_API_KEY}&ACTION=FETCH&OPTION=BETWEEN:{previous_str}+{now_str}'
            )
        else:
            logging.info(f'Getting all log...')
            res = requests.post('https://logbook.qrz.com/api', data=f'KEY={QRZ_API_KEY}&ACTION=FETCH')
        if res.ok:
            logging.info('Parsing the log and putting to database...')
            result_str = res.text.replace('&lt;','<').replace('&gt;','>').replace('\n', ' ')
            result_adif = re.search(r'ADIF=(.*<eor>)', result_str)
            if result_adif:
                adif_parser(result_adif.group(1))
    
    if MULTICAST:
        sock.bind(('', WSJTX_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(WSJTX_IP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    else:
        sock.bind((WSJTX_IP, WSJTX_PORT))

    states.receiver_started = True
    
    logging.info('Done Initializing!')

def main(sock: socket.socket, states_list: typing.Dict[str, States]):
    global IP_LOCK

    ip_from = None
    socks = [sock]

    init(sock, states_list[''])

    while True:
        try:
            t = select.select(socks, [], [], 0.5)
            fds, _, _ = typing.cast(typing.Tuple[typing.List[socket.socket], list, list], t)
            for fdin in fds:
                _data, ip_from = fdin.recvfrom(1024)
                if IP_LOCK and (IP_LOCK[0] != ip_from[0] or IP_LOCK[1] != ip_from[1]):
                    continue
                if not IP_LOCK:
                    IP_LOCK = [ip_from[0], ip_from[1]]
                    states_list[''].change_states(
                        ip = ip_from[0],
                        port = ip_from[1]
                    )
                    states_list[''].enable_monitoring()
                    states_list[''].change_frequency((MAX_FREQUENCY+MIN_FREQUENCY)//2)
                    states_list[''].use_RR73()
                process_wsjt(_data, ip_from, states_list[''])
        except KeyboardInterrupt:
            call_coll.delete_many({})
            message_coll.delete_many({})
            states_list[''].receiver_started = False
            break
        except:
            call_coll.delete_many({})
            message_coll.delete_many({})
            states_list[''].receiver_started = False
            logging.exception('Something not right!')
            break
    
if __name__ == '__main__':
    file_handlers = handlers.RotatingFileHandler(os.path.join(CURRENT_DIR, 'log', 'receiver.log'), maxBytes=10*1024*1024, backupCount=5)
    file_handlers.setLevel(logging.INFO)
    stream_handlers = logging.StreamHandler()
    stream_handlers.setLevel(logging.DEBUG if DEBUGGING else logging.INFO)
    logging.basicConfig(
        format='[%(asctime)s] [%(levelname)s] %(message)s',
        level=logging.DEBUG,
        handlers=[
            file_handlers,
            stream_handlers
        ])
    
    main(sock_wsjt, STATES_LIST)