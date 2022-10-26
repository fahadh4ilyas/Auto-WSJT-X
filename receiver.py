import re, socket, select, wsjtx, struct, requests

from datetime import datetime, timedelta

from pyhamtools.locator import calculate_distance, calculate_heading, latlong_to_locator
from pyhamtools.frequency import freq_to_band
from pymongo import MongoClient

from states import States
from config import *
from adif_parser import main as adif_parser, call_info, call_info2, country_to_dxcc
import logging
from logging import handlers

DXCC_EXCEPTION = [country_to_dxcc.get(i,0) for i in DXCC_EXCEPTION]

callsign_exc = []
if CALLSIGN_EXCEPTION:
    try:
        with open(CALLSIGN_EXCEPTION) as f:
            callsign_exc = f.read().splitlines()
    except:
        pass

if MULTICAST:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
else:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_wsjt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_wsjt.setblocking(False) # Set socket to non-blocking mode
sock_wsjt.setblocking(0)
bind_addr = socket.gethostbyname(WSJTX_IP)

states = States(REDIS_HOST, REDIS_PORT, multicast=MULTICAST)

mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client.wsjt
grid_coll = db.grid
call_coll = db.calls
hold_coll = db.holds
filtered_coll = db.filtered
done_coll = db.black

def filter_cq(data):
    if DXCC_EXCEPTION and 'dxcc' in data and data['dxcc'] in DXCC_EXCEPTION:
        return False
        
    if states.new_grid and 'grid' in data and not done_coll.find_one({'grid': data['grid'], 'band': data['band']}):
        return True
    
    if states.new_dxcc and 'dxcc' in data and not done_coll.find_one({'dxcc': data['dxcc'], 'band': data['band']}):
        return True
    
    if 'extra' in data and data['extra']:
        if data['extra'] == 'DX' and data.get('country', '') != 'Indonesia':
            return True
        if data['extra'] in ['OC', 'AS']:
            return True
    
    return False

def filter_blacklist(data):

    if WORK_ON_UNCONFIRMED_QSO:
        if done_coll.find_one({'callsign': data['callsign'], 'band': data['band'], 'confirmed': True}):
            return False
    else:
        if done_coll.find_one({'callsign': data['callsign'], 'band': data['band']}):
            return False
    
    return True

def completing_data(data, importance, aggresive, my_grid):

    try:
        location_data = call_info.get_all(data['complete_callsign'])
        data['country'] = location_data['country']
        data['dxcc'] = country_to_dxcc.get(location_data['country'], 0)
        data['continent'] = location_data['continent']
    except:
        location_data = {}
    if 'grid' in data:
        data['distance'] = calculate_distance(my_grid, data['grid'])
        data['azimuth'] = calculate_heading(my_grid, data['grid'])
    else:
        current_grid = (grid_coll.find_one({'callsign': data['callsign']}) or {}).get('grid', None)
        if not current_grid and 'latitude' in location_data:
            current_grid = latlong_to_locator(location_data['latitude'], location_data['longitude'])[:4]
        if current_grid:
            data['grid'] = current_grid
            data['distance'] = calculate_distance(my_grid, data['grid'])
            data['azimuth'] = calculate_heading(my_grid, data['grid'])
    data['importance'] = importance
    data['aggresive'] = aggresive

def process_wsjt(data, ip_from: tuple, states: States):
    global callsign_exc

    try:
        packet = wsjtx.ft8_decode(data)
    except (IOError, NotImplementedError) as err:
        return

    if isinstance(packet, wsjtx.WSHeartbeat):
        logging.info(f'IP: {ip_from[0]} | Port: {ip_from[1]}')
        states.ip = ip_from[0]
        states.port = ip_from[1]
        states.closed = False
        states.receiver_started = True
        if CALLSIGN_EXCEPTION:
            try:
                with open(CALLSIGN_EXCEPTION) as f:
                    callsign_exc = f.read().splitlines()
            except:
                pass
    
    elif isinstance(packet, wsjtx.WSStatus):
        logging.debug(packet)
        states.my_callsign = packet.DeCall or ''
        states.my_grid = packet.DeGrid or ''
        states.dx_callsign = packet.DXCall or ''
        states.dx_grid = packet.DXGrid or ''
        states.band = freq_to_band(packet.Frequency//1000)['band']
        states.tx_enabled = packet.TXEnabled
        states.decoding = packet.Decoding
        states.rxdf = packet.RXdf
        states.txdf = packet.TXdf
        states.tx_even = packet.TxEven
        packet_last_tx = packet.LastTxMsg or ''

        isDifferent = states.last_tx != packet_last_tx
        isTransmitting = packet.Transmitting and states.transmitting != packet.Transmitting

        states.transmitting = packet.Transmitting

        if isTransmitting:
            message = packet_last_tx
            message_types = ''
            matching = None
            for types, c in wsjtx.call_types:
                matching = c.match(message)
                if matching:
                    message_types = types
                    break
            if message_types == 'CQ':
                logging.info(f'[TX] [BAND: {states.band}] [FREQUENCY: {states.txdf}] CQ at {matching["grid"]}')
            elif message_types == 'REPLY':
                logging.info(f'[TX] [BAND: {states.band}] [FREQUENCY: {states.txdf}] Message to {matching["complete_to"]} at {matching["grid"]}')
            elif message_types == 'R73':
                logging.info(f'[TX] [BAND: {states.band}] [FREQUENCY: {states.txdf}] Last message to {matching["complete_to"]}: {matching["R73"]}')
            elif message_types == 'SNR':
                logging.info(f'[TX] [BAND: {states.band}] [FREQUENCY: {states.txdf}] Message to {matching["complete_to"]}: {matching["snr"]}')
            elif message_types == 'RSNR':
                logging.info(f'[TX] [BAND: {states.band}] [FREQUENCY: {states.txdf}] Message to {matching["complete_to"]}: R{matching["snr"]}')

        if isDifferent:
            states.last_tx = packet_last_tx
            matched = wsjtx.call_types[2][1].match(packet_last_tx)
            if matched:
                now = datetime.now().timestamp()
                qso_data = done_coll.find_one({'callsign': matched.group('to'), 'band': states.band}) or {}
                current_data = qso_data
                if not qso_data:
                    logging.info(f'Logging QSO: {matched.group("to")} at band {states.band}')
                    states.log_qso()
                if states.transmitter_started:
                    current_data = call_coll.find_one({'callsign': matched.group('to'), 'band': states.band}) or {}
                    current2_data = hold_coll.find_one({'callsign': matched.group('to'), 'band': states.band}) or {}
                    if not current_data:
                        current_data = current2_data
                else:
                    current_data = call_coll.find_one_and_delete({'callsign': matched.group('to'), 'band': states.band}) or {}
                    current2_data = hold_coll.find_one_and_delete({'callsign': matched.group('to'), 'band': states.band}) or {}
                    if not current_data:
                        current_data = current2_data
                if not current_data:
                    current_data['complete_callsign'] = matched.group('complete_to')
                    if not qso_data:
                        current_data['callsign'] = matched.group('to')
                        current_data['band'] = states.band
                        completing_data(current_data, 1, 0, states.my_grid)
                current_data['confirmed'] = True
                current_data['timestamp'] = now
                current_data['complete_callsign'] = matched.group('complete_to')
                current_data['callsign'] = matched.group('to')
                current_data['band'] = states.band
                if 'country' not in current_data:
                    completing_data(current_data, 1, 0, states.my_grid)
                if call_info2 and current_data['country'] == 'United States' and 'state' not in current_data:
                    try:
                        state_data = call_info2.get_all(current_data['complete_callsign'])
                    except:
                        state_data = {}
                    if 'state' in state_data:
                        current_data['state'] = state_data['state']
                    if 'county' in state_data:
                        current_data['county'] = state_data['county']
                current_data.pop('_id', None)
                done_coll.update_one({'callsign': matched.group('to'), 'band': states.band}, {'$set': current_data}, upsert=True)

    elif isinstance(packet, wsjtx.WSDecode):
        now = datetime.now().timestamp()
        message = packet.Message
        if MIN_FREQUENCY <= packet.DeltaFrequency <= MAX_FREQUENCY:
            delta_time = packet.Time/1000
            if 0 <= delta_time%30 < 15:
                states.add_even_frequency(packet.DeltaFrequency)
            else:
                states.add_odd_frequency(packet.DeltaFrequency)
        message_types = ''
        matching = None
        for types, c in wsjtx.call_types:
            matching = c.match(message)
            if matching:
                message_types = types
                break
        if not matching:
            return

        data = matching.groupdict().copy()
        data.update(packet.as_dict())
        data['band'] = states.band
        data['confirmed'] = False
        data['type'] = message_types
        data['timestamp'] = now
        data['expired'] = False
        my_grid = states.my_grid

        matching = data
        if message_types == 'CQ':
            logging.info(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
        elif message_types == 'REPLY':
            if matching['to'] == states.my_callsign:
                logging.warning(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
            else:
                logging.info(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
        elif message_types == 'R73':
            if matching['to'] == states.my_callsign:
                logging.warning(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
            else:
                logging.info(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
        elif message_types == 'SNR':
            if matching['to'] == states.my_callsign:
                logging.warning(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
            else:
                logging.info(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
        elif message_types == 'RSNR':
            if matching['to'] == states.my_callsign:
                logging.warning(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')
            else:
                logging.info(f'[RX] [BAND: {states.band}] [FREQUENCY: {packet.DeltaFrequency}] [DB: {packet.SNR}] {message}')

        if states.num_inactive_before_cut and data['callsign'] == states.current_callsign:
            states.inactive_count = 0

        if message_types == 'CQ':
            if data.get('grid', None):
                grid_coll.update_one({'callsign': data['callsign']}, {'$set': {'callsign': data['callsign'], 'grid': data['grid']}}, upsert=True)
            if not filter_blacklist(data):
                logging.info('The CQ is already blacklisted!')
                return
            if data['callsign'] in callsign_exc:
                logging.info('The Callsign is already blacklisted!')
                return
            if not states.include_no_grid_cq and not data.get('grid', None):
                logging.info('The CQ has no grid!')
                return
            if not states.include_callsign_with_affix and data['complete_callsign'] != data['callsign']:
                logging.info('This Callsign has affix!')
                return
            completing_data(data, 1, 0, my_grid)
            if not filter_cq(data):
                logging.info('The CQ is not follow criteria!')
                filtered_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
            else:
                # check_data = call_coll.find_one({'callsign': data['callsign'], 'band': data['band']})
                # check2_data = hold_coll.find_one_and_delete({'callsign': data['callsign'], 'band': data['band']})
                # if states.current_callsign != data['callsign']:
                #     if check_data:
                #         data['importance'] = check_data['importance'] + 1
                #     elif check2_data:
                #         data['importance'] = check2_data['importance'] + 1
                logging.info('Deleting callsign from hold call database!')
                hold_coll.delete_one({'callsign': data['callsign'], 'band': data['band']})
                if 'dxcc' in data and not done_coll.find_one({'dxcc': data['dxcc'], 'band': data['band']}) and states.current_callsign != data['callsign']:
                    data['importance'] = 2
                    states.next_callsign = data['callsign']
                logging.info('Adding callsign to call list database!')
                call_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
        elif message_types == 'R73':
            if data['to'] == states.my_callsign: 
                if data['R73'] == '73':
                    logging.info('Deleting callsign from hold call and call list database!')
                    data['confirmed'] = True
                    check_data = call_coll.find_one_and_delete({'callsign': data['callsign'], 'band': data['band']}) or {}
                    check2_data = hold_coll.find_one_and_delete({'callsign': data['callsign'], 'band': data['band']}) or {}
                    data = {**check_data, **check2_data, **data}
                    data.pop('_id', None)
                    logging.info('Adding callsign to blacklist database!')
                    done_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
                else:
                    logging.info('Deleting callsign from hold call database!')
                    completing_data(data, 3, 0, my_grid)
                    check_data = call_coll.find_one({'callsign': data['callsign'], 'band': data['band']})
                    check2_data = hold_coll.find_one_and_delete({'callsign': data['callsign'], 'band': data['band']})
                    if check_data:
                        data['importance'] = max(check_data['importance'] + 1, 3)
                    elif check2_data:
                        data['importance'] = max(check2_data['importance'] + 1, 3)
                    current_callsign = states.current_callsign
                    if current_callsign != '' and data['callsign'] != current_callsign and states.next_callsign == '':
                        states.next_callsign = data['callsign']
                    logging.info('Adding callsign to call list database!')
                    call_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
                return
            if not states.rr73_as_cq:
                return
            if not filter_blacklist(data):
                logging.info('The 73 is already blacklisted!')
                return
            if data['callsign'] in callsign_exc:
                logging.info('The Callsign is already blacklisted!')
                return
            if not states.include_callsign_with_affix and data['complete_callsign'] != data['callsign']:
                logging.info('This Callsign has affix!')
                return
            completing_data(data, 1, 0, my_grid)
            if not filter_cq(data):
                logging.info('The 73 is not follow criteria!')
                filtered_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
            else:
                # check_data = call_coll.find_one({'callsign': data['callsign'], 'band': data['band']})
                # check2_data = hold_coll.find_one_and_delete({'callsign': data['callsign'], 'band': data['band']})
                # if states.current_callsign != data['callsign']:
                #     if check_data:
                #         data['importance'] = check_data['importance'] + 1
                #     elif check2_data:
                #         data['importance'] = check2_data['importance'] + 1
                logging.info('Deleting callsign from hold call database!')
                hold_coll.delete_one({'callsign': data['callsign'], 'band': data['band']})
                if 'dxcc' in data and not done_coll.find_one({'dxcc': data['dxcc'], 'band': data['band']}) and states.current_callsign != data['callsign']:
                    data['importance'] = 2
                    states.next_callsign = data['callsign']
                logging.info('Adding callsign to call list database!')
                call_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
        else:
            if 'grid' in data:
                grid_coll.update_one({'callsign': data['callsign']}, {'$set': {'callsign': data['callsign'], 'grid': data['grid']}}, upsert=True)
            check_data = call_coll.find_one({'callsign': data['callsign'], 'band': states.band})
            check2_data = hold_coll.find_one({'callsign': data['callsign'], 'band': states.band})
            if check_data:
                logging.info('Callsign is already in call list database')
                if data['to'] == states.my_callsign:
                    logging.info('updating callsign data in call list database!')
                    data['importance'] = check_data['importance'] + 1
                    data['aggresive'] = 0
                    call_coll.update_one({'callsign': data['callsign'], 'band': states.band}, {'$set': data})
                elif states.max_force_reply_when_busy:
                    logging.info('updating callsign data in call list database!')
                    check_data['importance'] += 1
                    check_data['timestamp'] = data['timestamp']
                    check_data['expired'] = False
                    call_coll.update_one({'callsign': data['callsign'], 'band': states.band}, {'$set': check_data})
                else:
                    logging.info('Deleting callsign from call list database!')
                    call_coll.delete_one({'callsign': data['callsign']})
            elif check2_data:
                logging.info('Callsign is already in hold call database')
                if data['to'] == states.my_callsign:
                    logging.info('Move callsign data from hold call to call list database!')
                    data['importance'] = check2_data['importance'] + 1
                    data['aggresive'] = 0
                    hold_coll.delete_one({'callsign': data['callsign'], 'band': states.band})
                    call_coll.update_one({'callsign': data['callsign'], 'band': states.band}, {'$set': data}, upsert=True)
                elif check2_data['aggresive'] < states.max_force_reply_when_busy:
                    logging.info('Move callsign data from hold call to call list database while aggresive!')
                    check2_data['importance'] += 1
                    check2_data['aggresive'] += 1
                    check2_data['timestamp'] = data['timestamp']
                    check2_data['expired'] = False
                    hold_coll.delete_one({'callsign': data['callsign'], 'band': states.band})
                    call_coll.update_one({'callsign': data['callsign'], 'band': states.band}, {'$set': check2_data})
            else:
                if data['to'] == states.my_callsign:
                    logging.info('Adding callsign to call list database!')
                    if data['type'] == 'RSNR':
                        completing_data(data, 2, 0, my_grid)
                    else:
                        completing_data(data, 0, 0, my_grid)
                    call_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)
                elif states.max_force_reply_when_busy:
                    completing_data(data, 1, 0, my_grid)
                    if 'dxcc' in data and not done_coll.find_one({'dxcc': data['dxcc'], 'band': data['band']}) and states.current_callsign != data['callsign']:
                        logging.info('Adding new DXCC callsign to call list database!')
                        data['type'] = 'CQ'
                        states.next_callsign = data['callsign']
                        call_coll.update_one({'callsign': data['callsign'], 'band': data['band']}, {'$set': data}, upsert=True)

    elif isinstance(packet, wsjtx.WSADIF):
        logging.info(f'LOGGED ADIF: {packet.ADIF}')

    elif isinstance(packet, wsjtx.WSClose):
        logging.warning(packet)
        states.closed = True
        raise KeyboardInterrupt('WSJT-X Closed!')

def init(sock: socket.socket):

    logging.info('Initializing...')
    states.r.flushdb()
    states.min_db = MIN_DB
    states.include_no_grid_cq = INCLUDE_NO_GRID_CQ
    states.include_callsign_with_affix = INCLUDE_CALLSIGN_WITH_AFFIX
    states.new_grid = NEW_GRID
    states.new_dxcc = NEW_DXCC
    states.rr73_as_cq = RR73_AS_CQ
    states.max_force_reply_when_busy = MAX_FORCE_REPLY_WHEN_BUSY
    states.num_inactive_before_cut = NUM_INACTIVE_BEFORE_CUT

    if QRZ_API_KEY:
        logging.info('Checking QRZ Logbook...')
        if NUM_DAYS_LOG:
            now = datetime.now()
            previous = now - timedelta(days=NUM_DAYS_LOG)
            now_str = now.strftime('%Y-%m-%d')
            previous_str = previous.strftime('%Y-%m-%d')
            logging.info(f'Getting log from {previous_str} to {now_str}...')
            res = requests.post('https://logbook.qrz.com/api',data=f'KEY={QRZ_API_KEY}&ACTION=FETCH&OPTION=BETWEEN:{previous_str}+{now_str}')
        else:
            logging.info(f'Getting all log...')
            res = requests.post('https://logbook.qrz.com/api',data=f'KEY={QRZ_API_KEY}&ACTION=FETCH')
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

def main(sock: socket.socket, states: States):

    ip_from = None
    sock = [sock]

    init(sock[0])

    while True:
        try:
            fds, _, _ = select.select(sock, [], [], 0.5)
            for fdin in fds:
                data, ip_from = fdin.recvfrom(1024)
                process_wsjt(data, ip_from, states)
        except KeyboardInterrupt:
            call_coll.delete_many({})
            hold_coll.delete_many({})
            filtered_coll.delete_many({})
            states.receiver_started = False
            break
        except:
            call_coll.delete_many({})
            hold_coll.delete_many({})
            filtered_coll.delete_many({})
            states.receiver_started = False
            logging.exception('Something not right!')
            break
    
if __name__ == '__main__':
    file_handlers = handlers.RotatingFileHandler('log/wsjtx.log', maxBytes=2*1024*1024, backupCount=5)
    file_handlers.setLevel(logging.INFO)
    logging.basicConfig(
        format='[%(levelname)s] [%(asctime)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[
            file_handlers,
            logging.StreamHandler()
        ])
    
    main(sock_wsjt, states)