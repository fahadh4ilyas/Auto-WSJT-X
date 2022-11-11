import os, socket, select, wsjtx, struct, typing

from pyhamtools.frequency import freq_to_band

from config import CURRENT_DIR, MULTICAST, WSJTX_IP, WSJTX_PORT, DEBUGGING
import logging
from handler import RollingFileHandler

LOCAL_STATES = {
    'band': 0,
    'mode': '',
    'transmitting': False,
    'current_tx': ''
}

if MULTICAST:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
else:
    sock_wsjt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_wsjt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_wsjt.setblocking(False) # Set socket to non-blocking mode
sock_wsjt.setblocking(0)
bind_addr = socket.gethostbyname(WSJTX_IP)

def process_wsjt(_data: bytes, ip_from: tuple):
    global LOCAL_STATES

    try:
        packet = wsjtx.ft8_decode(_data)
    except (IOError, NotImplementedError):
        logging.exception('Something not right!')
        return

    if isinstance(packet, wsjtx.WSHeartbeat):

        logging.info(f'[HOST: {ip_from[0]}:{ip_from[1]}] Hearbeat...')
    
    elif isinstance(packet, wsjtx.WSStatus):

        logging.debug(f'[HOST: {ip_from[0]}:{ip_from[1]}] {packet}')
        packet_last_tx = packet.LastTxMsg or ''
        isTransmitting = packet.Transmitting and LOCAL_STATES['current_tx'] != packet_last_tx

        LOCAL_STATES['transmitting'] = packet.Transmitting
        LOCAL_STATES['current_tx'] = packet_last_tx 
        LOCAL_STATES['band'] = freq_to_band((packet.Frequency or 135000)//1000)['band']
        LOCAL_STATES['mode'] = packet.Mode or ''

        if isTransmitting:

            logging.info(
                f'[HOST: {ip_from[0]}:{ip_from[1]}] [TX] [MODE: {LOCAL_STATES["mode"]}] [BAND: {LOCAL_STATES["band"]}] '
                f'[FREQUENCY: {packet.TXdf}] {packet.LastTxMsg}'
            )

    elif isinstance(packet, wsjtx.WSDecode):

        hours, remainder = divmod(packet.Time//1000, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logging.info(
            f'[HOST: {ip_from[0]}:{ip_from[1]}] [RX] [MODE: {LOCAL_STATES["mode"]}] [BAND: {LOCAL_STATES["band"]}] '
            f'[FREQUENCY: {packet.DeltaFrequency}] [UTC: {hours:02d}{minutes:02d}{seconds:02d}] [DT: {packet.DeltaTime}] '
            f'[DB: {packet.SNR}] {packet.Mode} {packet.Message}'
        )

    elif isinstance(packet, wsjtx.WSADIF):
        logging.info(f'[HOST: {ip_from[0]}:{ip_from[1]}] LOGGED ADIF: {packet.ADIF}')

    elif isinstance(packet, wsjtx.WSClose):
        logging.warning(f'[HOST: {ip_from[0]}:{ip_from[1]}] CLOSED!!!')
        LOCAL_STATES[f'{ip_from[0]}:{ip_from[1]}'] = False

    else:
        logging.debug(f'[HOST: {ip_from[0]}:{ip_from[1]}] {packet}')

def init(sock: socket.socket):

    logging.info('Initializing...')
    
    if MULTICAST:
        sock.bind(('', WSJTX_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(WSJTX_IP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    else:
        sock.bind((WSJTX_IP, WSJTX_PORT))

    logging.info('Done Initializing!')

def main(sock: socket.socket):
    global LOCAL_STATES

    ip_from = None
    socks = [sock]

    init(sock)

    while True:
        try:
            t = select.select(socks, [], [], 0.5)
            fds, _, _ = typing.cast(typing.Tuple[typing.List[socket.socket], list, list], t)
            for fdin in fds:
                _data, ip_from = fdin.recvfrom(1024)
                ip_str = f'{ip_from[0]}:{ip_from[1]}'
                if not LOCAL_STATES.get(ip_str, False):
                    logging.warning(f'[HOST: {ip_str}] OPENED!!!')
                    LOCAL_STATES[ip_str] = True
                process_wsjt(_data, ip_from)
        except KeyboardInterrupt:
            break
        except:
            logging.exception('Something not right!')
            break
    
if __name__ == '__main__':
    file_handlers = RollingFileHandler(os.path.join(CURRENT_DIR, 'log', 'message.log'), maxBytes=10*1024*1024)
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
    
    main(sock_wsjt)