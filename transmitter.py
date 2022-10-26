import re, time, wsjtx, traceback
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

CURRENT_DATA = {}
TRANSMIT_COUNTER = 0
LATEST_TX = ''

def calculate_best_frequency(freq: list):

    d = sorted(set(freq))

    curr_max = 0
    curr_best = 0

    for left,right in zip(d, d[1:]):
        if right - left > curr_max:
            curr_max = right - left
            curr_best = (right+left)//2
    
    return curr_best

def checking_band():
    global CURRENT_DATA

    while CURRENT_DATA and CURRENT_DATA['band'] != states.band:
        logging.warning('Band changing!')
        call_coll.delete_many({'band': CURRENT_DATA['band']})
        hold_coll.delete_many({'band': CURRENT_DATA['band'], 'importance': {'$lte': 1}})
        last_tx = wsjtx.call_types[2][1].match(states.last_tx)
        if last_tx and last_tx.group('to') == CURRENT_DATA['callsign']:
            done_data = done_coll.find_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}) or {}
            done_data.pop('_id', None)
            if not (done_data and done_data['confirmed']):
                done_coll.update_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}, {'$set': {**done_data, **CURRENT_DATA}}, upsert=True)
        elif CURRENT_DATA['to'] == states.my_callsign:
            hold_coll.update_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}, {'$set': CURRENT_DATA}, upsert=True)
        CURRENT_DATA = call_coll.find_one({'band': states.band, 'expired': False, 'SNR': {'$gt': states.min_db}}, sort=states.sort_by) or {}
        CURRENT_DATA.pop('_id', None)

def checking_callsign():
    global CURRENT_DATA

    next_callsign = states.next_callsign
    while CURRENT_DATA and next_callsign != '' and CURRENT_DATA['callsign'] != next_callsign:
        logging.warning('Force Callsign changing!')
        CURRENT_DATA = call_coll.find_one({'band': states.band, 'callsign': next_callsign}, sort=states.sort_by) or {}
        if not CURRENT_DATA:
            CURRENT_DATA = hold_coll.find_one_and_delete({'band': states.band, 'callsign': next_callsign}, sort=states.sort_by) or {}
            if not CURRENT_DATA:
                CURRENT_DATA = filtered_coll.find_one_and_delete({'band': states.band, 'callsign': next_callsign}, sort=states.sort_by) or {}
            if CURRENT_DATA:
                CURRENT_DATA['importance'] += 1
        if CURRENT_DATA:
            CURRENT_DATA.pop('_id', None)
            call_coll.update_one({'band': states.band, 'callsign': next_callsign}, {'$set': CURRENT_DATA})
    states.next_callsign = ''

def replying(renew_frequency: bool = True):
    global CURRENT_DATA

    states.current_callsign = CURRENT_DATA['callsign']
    isOdd = (0 <= CURRENT_DATA['Time']/1000%30 < 15)
    if isOdd:
        frequencies = states.even_frequencies
    else:
        frequencies = states.odd_frequencies
    best_frequency = None
    if renew_frequency:
        best_frequency = calculate_best_frequency(frequencies)
    states.reply(CURRENT_DATA, best_frequency)
    if states.num_inactive_before_cut:
        states.inactive_count += 1
    states.transmit_phase = True
    logging.info('Replying to: '+CURRENT_DATA['callsign'])
    return True

def reset_frequency_list():
    states.even_frequencies = [MIN_FREQUENCY, MAX_FREQUENCY]
    states.odd_frequencies = [MIN_FREQUENCY, MAX_FREQUENCY]

def one_transmit(new: bool = True):
    global CURRENT_DATA

    if new:
        CURRENT_DATA = call_coll.find_one({'band': states.band, 'expired': False, 'SNR': {'$gt': states.min_db}}, sort=states.sort_by) or {}
        CURRENT_DATA.pop('_id', None)
    states.tries = 0
    checking_band()
    checking_callsign()
    if CURRENT_DATA:
        return replying()
    states.current_callsign = ''
    states.disable_transmit()
    states.enable_monitoring()
    states.transmit_phase = False
    return False

def restarting_transmit(put_hold: bool = True, NEW_DATA: dict = {}):
    global CURRENT_DATA

    if not NEW_DATA:
        NEW_DATA = call_coll.find_one_and_delete({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']})
    else:
        call_coll.delete_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']})
    matched = wsjtx.call_types[2][1].match(states.last_tx)
    if matched:
        states.disable_transmit()
        done_data = done_coll.find_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}) or {}
        done_data.pop('_id', None)
        if not (done_data and done_data['confirmed']):
            NEW_DATA['confirmed'] = True
            done_coll.update_one({'callsign': NEW_DATA['callsign'], 'band': NEW_DATA['band']}, {'$set': {**done_data, **NEW_DATA}}, upsert=True)
    if put_hold and not matched or matched.group('R73') != '73':
        logging.warning('Putting reply on hold!')
        hold_coll.update_one({'callsign': NEW_DATA['callsign'], 'band': NEW_DATA['band']}, {'$set': NEW_DATA}, upsert=True)
    logging.info('Getting new call to reply')
    one_transmit()
    reset_frequency_list()


def transmitting(now):
    global CURRENT_DATA, LATEST_TX, TRANSMIT_COUNTER

    call_coll.update_many({'timestamp': {'$lte': now-EXPIRED_TIME}, 'importance': {'$lte': 1}}, {'$set': {'expired': True}})
    hold_coll.update_many({'timestamp': {'$lte': now-EXPIRED_TIME}, 'importance': {'$lte': 1}}, {'$set': {'expired': True}})
    if states.transmit_phase:
        states.enable_monitoring()
        states.transmit_phase = False
        states.tries = states.tries + 1
        if states.last_tx == LATEST_TX:
            TRANSMIT_COUNTER += 1
        else:
            LATEST_TX = states.last_tx
            TRANSMIT_COUNTER = 1
        time.sleep(0.5)
        return
    next_callsign = states.next_callsign
    if not CURRENT_DATA:
        logging.info('Getting new call to reply')
        states.disable_transmit()
        one_transmit()
        reset_frequency_list()
    elif CURRENT_DATA['band'] != states.band or (next_callsign != '' and CURRENT_DATA['callsign'] != next_callsign):
        one_transmit(new=False)
        reset_frequency_list()
    elif TRANSMIT_COUNTER >= max(states.max_force_reply_when_busy+1,2)*states.max_tries:
        TRANSMIT_COUNTER = 1
        logging.warning('Looping transmit detected!')
        restarting_transmit(False)
    elif states.num_inactive_before_cut and states.inactive_count >= states.num_inactive_before_cut:
        states.inactive_count = 0
        logging.warning(f'{states.current_callsign} is not active!')
        restarting_transmit()
    else:
        NEW_DATA = call_coll.find_one({'band': CURRENT_DATA['band'], 'callsign': CURRENT_DATA['callsign']}) or {}
        NEW_DATA.pop('_id', None)
        if not NEW_DATA:
            logging.warning('The person we replied probably is talking with some one else')
            matched = wsjtx.call_types[2][1].match(states.last_tx)
            if matched:
                states.disable_transmit()
            logging.info('Getting new call to reply')
            one_transmit()
            reset_frequency_list()
            return
        elif CURRENT_DATA['type'] != NEW_DATA['type']:
            if NEW_DATA['type'] == 'CQ' and re.match(r'^.*(RRR|R*73)', states.last_tx):
                logging.warning('The person we replied already CQing')
                CURRENT_DATA['confirmed'] = True
                call_coll.delete_one({'callsign': NEW_DATA['callsign'], 'band': NEW_DATA['band']})
                done_coll.update_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}, {'$set': CURRENT_DATA}, upsert=True)
                logging.info('Getting new call to reply')
                one_transmit()
                reset_frequency_list()
                return
            else:
                states.tries = 0
        tries = states.tries
        if tries >= states.max_tries or (NEW_DATA.get('R73', '73') != '73' and NEW_DATA['to'] == states.my_callsign and tries == 1):
            logging.warning('Maximum tries achieved!')
            restarting_transmit(NEW_DATA=NEW_DATA)
        else:
            CURRENT_DATA = NEW_DATA
            replying(renew_frequency=True if tries%states.max_tries_change_freq == 0 else False)
            reset_frequency_list()

def init():
    logging.info('Initializing...')
    states.transmitter_started = True
    states.sort_by = SORTBY
    states.current_callsign = ''
    states.next_callsign = ''
    states.transmit_phase = False
    states.max_tries = MAX_TRIES
    states.max_tries_change_freq = MAX_TRIES_CHANGE_FREQUENCY
    states.change_frequency((MAX_FREQUENCY+MIN_FREQUENCY)//2)
    states.enable_monitoring()

def main():
    global CURRENT_DATA
    
    logging.info('Waiting for receiver receive heartbeat...')
    while not states.receiver_started:
        now = datetime.now().timestamp()
        time.sleep(0.5)

    init()
    while True:
        try:
            if states.closed:
                raise ValueError('WSJT-X Closed!')
            now = datetime.now().timestamp()
            if now%15 < 14.8:
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
            if CURRENT_DATA:
                latest_data = call_coll.find_one_and_delete({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']})
                hold_coll.update_one({'callsign': CURRENT_DATA['callsign'], 'band': CURRENT_DATA['band']}, {'$set': latest_data}, upsert=True)
                CURRENT_DATA = {}
            if input('Stop transmit? (y/n) ') == 'y':
                break
            states.transmitter_started = True
        except:
            states.transmitter_started = False
            states.halt_transmit()
            states.disable_transmit()
            logging.exception('Something not right!')
            break

if __name__ == '__main__':
    file_handlers = handlers.RotatingFileHandler('log/transmitter.log', maxBytes=2*1024*1024, backupCount=5)
    file_handlers.setLevel(logging.INFO)
    logging.basicConfig(
        format='[%(levelname)s] [%(asctime)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[
            file_handlers,
            logging.StreamHandler()
        ])
    
    main()