import time, typing
from datetime import datetime
from pymongo import MongoClient
from states import States
from config import *
import logging
from logging import handlers

mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client.wsjt
call_coll = db.calls
hold_coll = db.holds
filtered_coll = db.filtered
done_coll = db.black

STATES_LIST: typing.Dict[str, States] = {
    '': States(REDIS_HOST, REDIS_PORT, MULTICAST)
}

IS_EVEN = None

class TransmitterPaused(Exception):
    pass

def calculate_best_frequency(freq: list) -> int:

    d = sorted(set(freq))

    curr_max = 0
    curr_best = 0

    for left,right in zip(d, d[1:]):
        if right - left > curr_max:
            curr_max = right - left
            curr_best = (right+left)//2
    
    return curr_best

def replying(states: States, CURRENT_DATA: dict, txOdd: bool, renew_frequency: bool = True) -> bool:

    if txOdd:
        frequencies = states.even_frequencies
    else:
        frequencies = states.odd_frequencies
    best_frequency = None
    if renew_frequency:
        logging.info('Finding best frequency')
        best_frequency = calculate_best_frequency(frequencies)
    states.current_callsign = CURRENT_DATA['callsign']
    states.reply(CURRENT_DATA, best_frequency, CURRENT_DATA.get('skipGrid', True), txOdd)
    states.transmit_phase = True
    logging.info('Replying to: '+CURRENT_DATA['callsign'])
    return True

def transmitting(now: float, states: States):
    global IS_EVEN

    if states.transmitter_paused:
        raise TransmitterPaused()

    if states.transmit_phase:
        states.enable_monitoring()
        states.transmit_phase = False
        time.sleep(0.5)
        return

    STATES_LIST_LOCAL = states.get_states(
        'band',
        'mode',
        'tries',
        'max_tries_change_freq'
    )
    
    logging.info('Finding new message to reply')
    if IS_EVEN is None:
        CURRENT_DATA = call_coll.find_one({
            'mode': STATES_LIST_LOCAL['mode'],
            'band': STATES_LIST_LOCAL['band'],
            'expired': False,
            'tried': False,
            'isSpam': False},
            sort=states.sort_by) or {}
    else:
        CURRENT_DATA = call_coll.find_one({
            'mode': STATES_LIST_LOCAL['mode'],
            'band': STATES_LIST_LOCAL['band'],
            'expired': False,
            'tried': False,
            'isSpam': False,
            'isEven': IS_EVEN},
            sort=states.sort_by) or {}
    
    if not CURRENT_DATA:
        IS_EVEN = None
        states.current_callsign = ''
        states.enable_transmit_counter = 0
        if states.ip != '':
            states.disable_transmit()
            states.clear_message()
            states.enable_monitoring()
        return
    
    message_time = CURRENT_DATA['isEven']
    current_time = (0 <= now%TIMING[CURRENT_DATA['mode']]['full'] < TIMING[CURRENT_DATA['mode']]['half'])
    if message_time != current_time:
        return
    
    IS_EVEN = message_time
        
    replying(states, CURRENT_DATA, IS_EVEN, STATES_LIST_LOCAL['tries']%STATES_LIST_LOCAL['max_tries_change_freq'] == 0)
    time.sleep(TIMING[CURRENT_DATA['mode']]['half']/2)

def init(states: States):
    logging.info('Initializing...')
    states.transmitter_started = True
    states.sort_by = SORTBY
    states.max_tries_change_freq = MAX_TRIES_CHANGE_FREQUENCY
    logging.info('Done Initializing!')

def main(states_list: typing.Dict[str, States]):
    global IS_EVEN
    
    logging.info('Waiting for receiver receive heartbeat...')
    while not states_list[''].receiver_started:
        now = datetime.now().timestamp()
        time.sleep(0.5)

    init(states_list[''])
    while True:
        try:
            if states_list[''].closed:
                raise ValueError('WSJT-X Closed!')
            now = datetime.now().timestamp()
            if now%TIMING['FT8']['half'] < TIMING['FT8']['half'] - 0.1:
                time.sleep(0.02)
                continue
            if not states_list[''].receiver_started:
                raise ValueError('Receiver Stopped!')
            transmitting(now, states_list[''])
            time.sleep(0.5)
        except TransmitterPaused:
            states_list[''].transmitter_started = False
            states_list[''].transmit_phase = False
            IS_EVEN = None
            if input('Resume transmit? (y/n) ') == 'n':
                break
            states_list[''].transmitter_started = True
            states_list[''].transmitter_paused = False
        except KeyboardInterrupt:
            states_list[''].transmitter_started = False
            states_list[''].transmit_phase = False
            IS_EVEN = None
            for k, states in states_list.items():
                states.halt_transmit()
                states.disable_transmit()
                states.clear_message()
            if input('Stop transmit? (y/n) ') == 'y':
                break
            states_list[''].transmitter_started = True
            states_list[''].transmitter_paused = False
        except:
            states_list[''].transmitter_started = False
            for k, states in states_list.items():
                states.halt_transmit()
                states.disable_transmit()
                states.clear_message()
            logging.exception('Something not right!')
            break

if __name__ == '__main__':
    file_handlers = handlers.RotatingFileHandler(os.path.join(CURRENT_DIR, 'log', 'transmitter.log'), maxBytes=10*1024*1024, backupCount=5)
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
    
    main(STATES_LIST)