import time
from datetime import datetime
from pymongo import MongoClient
from states import States
from config import *
import logging
from logging import handlers

states = States(REDIS_HOST, REDIS_PORT, multicast=MULTICAST)

mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client.wsjt
call_coll = db.calls
hold_coll = db.holds
filtered_coll = db.filtered
done_coll = db.black

IS_EVEN = None
STATES_LIST = {}

def calculate_best_frequency(freq: list) -> int:

    d = sorted(set(freq))

    curr_max = 0
    curr_best = 0

    for left,right in zip(d, d[1:]):
        if right - left > curr_max:
            curr_max = right - left
            curr_best = (right+left)//2
    
    return curr_best

def replying(CURRENT_DATA: dict, txOdd: bool, renew_frequency: bool = True) -> bool:

    if txOdd:
        frequencies = states.even_frequencies
    else:
        frequencies = states.odd_frequencies
    best_frequency = None
    if renew_frequency:
        logging.info('Finding best frequency')
        best_frequency = calculate_best_frequency(frequencies)
    states.reply(CURRENT_DATA, best_frequency, CURRENT_DATA.get('skipGrid', True), txOdd)
    states.transmit_phase = True
    logging.info('Replying to: '+CURRENT_DATA['callsign'])
    return True

def transmitting(now: float):
    global IS_EVEN, STATES_LIST

    if states.transmit_phase:
        states.enable_monitoring()
        states.transmit_phase = False
        time.sleep(0.5)
        return

    STATES_LIST = states.get_states(
        'band',
        'mode',
        'tries',
        'max_tries_change_freq'
    )
    
    logging.info('Finding new message to reply')
    if IS_EVEN is None:
        CURRENT_DATA = call_coll.find_one({
            'mode': STATES_LIST['mode'],
            'band': STATES_LIST['band'],
            'expired': False,
            'tried': False,
            'isSpam': False},
            sort=states.sort_by) or {}
    else:
        CURRENT_DATA = call_coll.find_one({
            'mode': STATES_LIST['mode'],
            'band': STATES_LIST['band'],
            'expired': False,
            'tried': False,
            'isSpam': False,
            'isEven': IS_EVEN},
            sort=states.sort_by) or {}
    
    if not CURRENT_DATA:
        IS_EVEN = None
        states.enable_transmit_counter = 0
        states.disable_transmit()
        states.clear_message()
        states.enable_monitoring()
        return
    
    message_time = CURRENT_DATA['isEven']
    current_time = (0 <= now%TIMING[CURRENT_DATA['mode']]['full'] < TIMING[CURRENT_DATA['mode']]['half'])
    if message_time != current_time:
        return
    
    IS_EVEN = message_time
        
    replying(CURRENT_DATA, IS_EVEN, STATES_LIST['tries']%STATES_LIST['max_tries_change_freq'] == 0)

def init():
    logging.info('Initializing...')
    states.transmitter_started = True
    states.sort_by = SORTBY
    states.max_tries_change_freq = MAX_TRIES_CHANGE_FREQUENCY
    states.enable_monitoring()
    states.change_frequency((MAX_FREQUENCY+MIN_FREQUENCY)//2)

def main():
    global CURRENT_DATA
    
    logging.info('Waiting for receiver receive heartbeat...')
    while not states.receiver_started or states.mode == '':
        now = datetime.now().timestamp()
        time.sleep(0.5)

    init()
    while True:
        try:
            if states.closed:
                raise ValueError('WSJT-X Closed!')
            now = datetime.now().timestamp()
            if now%TIMING[states.mode]['half'] < TIMING[states.mode]['half'] - 0.1:
                time.sleep(0.02)
                continue
            if not states.receiver_started:
                raise ValueError('Receiver Stopped!')
            transmitting(now)
            time.sleep(0.5)
        except KeyboardInterrupt:
            states.transmitter_started = False
            states.halt_transmit()
            states.disable_transmit()
            states.clear_message()
            if input('Stop transmit? (y/n) ') == 'y':
                break
            states.transmitter_started = True
        except:
            states.transmitter_started = False
            states.halt_transmit()
            states.disable_transmit()
            states.clear_message()
            logging.exception('Something not right!')
            break

if __name__ == '__main__':
    file_handlers = handlers.RotatingFileHandler('log/transmitter.log', maxBytes=10*1024*1024, backupCount=5)
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
    
    main()