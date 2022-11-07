import json, redis, socket, wsjtx

from enum import Enum

class TxSequence(Enum):
    EVEN = 14
    ODD = 15

class States(object):
    
    def __init__(self, redis_host: str = '127.0.0.1', redis_port: int = 6379, multicast: bool = False):
        
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0)

        if multicast:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        self.states_types = {
            'ip': str,
            'port': int,
            'my_callsign': str,
            'my_grid': str,
            'dx_callsign': str,
            'dx_grid': str,
            'band': int,
            'mode': str,
            'tx_enabled': bool,
            'transmitting': bool,
            'decoding': bool,
            'closed': bool,
            'rxdf': int,
            'txdf': int,
            'last_tx': str,
            'tx_even': bool,
            'transmitter_started': bool,
            'receiver_started': bool,
            'transmit_phase': bool,
            'current_callsign': str,
            'inactive_count': int,
            'tries': int,
            'transmit_counter': int,
            'enable_transmit_counter': int,
            'num_inactive_before_cut': int,
            'num_disable_transmit': int,
            'max_tries': int,
            'max_tries_change_freq': int,
            'min_db': int,
            'new_grid': bool,
            'new_dxcc': bool
        }

    # WSJT-X PARAMS
    # ==========================================================
    @property
    def ip(self) -> str:
        return (self.r.get('ip') or b'').decode()
    
    @ip.setter
    def ip(self, val: str):
        self.r.set('ip', val)
    
    @property
    def port(self) -> int:
        return int(self.r.get('port') or 0)
    
    @port.setter
    def port(self, val: int):
        self.r.set('port', val)

    @property
    def my_callsign(self) -> str:
        return (self.r.get('my_callsign') or b'').decode()
    
    @my_callsign.setter
    def my_callsign(self, val: str):
        self.r.set('my_callsign', val)

    @property
    def my_grid(self) -> str:
        return (self.r.get('my_grid') or b'').decode()
    
    @my_grid.setter
    def my_grid(self, val: str):
        self.r.set('my_grid', val)

    @property
    def dx_callsign(self) -> str:
        return (self.r.get('dx_callsign') or b'').decode()
    
    @dx_callsign.setter
    def dx_callsign(self, val: str):
        self.r.set('dx_callsign', val)

    @property
    def dx_grid(self) -> str:
        return (self.r.get('dx_grid') or b'').decode()
    
    @dx_grid.setter
    def dx_grid(self, val: str):
        self.r.set('dx_grid', val)
    
    @property
    def band(self) -> int:
        return int(self.r.get('band') or 0)
    
    @band.setter
    def band(self, val: int):
        self.r.set('band', val)

    @property
    def mode(self) -> str:
        return (self.r.get('mode') or b'').decode()
    
    @mode.setter
    def mode(self, val: str):
        self.r.set('mode', val)

    @property
    def tx_enabled(self) -> bool:
        return not not self.r.get('tx_enabled')
    
    @tx_enabled.setter
    def tx_enabled(self, val: bool):
        self.r.set('tx_enabled', 1 if val else '')
    
    @property
    def transmitting(self) -> bool:
        return not not self.r.get('transmitting')
    
    @transmitting.setter
    def transmitting(self, val: bool):
        self.r.set('transmitting', 1 if val else '')
        
    @property
    def decoding(self) -> bool:
        return not not self.r.get('decoding')
    
    @decoding.setter
    def decoding(self, val: bool):
        self.r.set('decoding', 1 if val else '')
    
    @property
    def closed(self) -> bool:
        return not not self.r.get('closed')
    
    @closed.setter
    def closed(self, val: bool):
        self.r.set('closed', 1 if val else '')

    @property
    def rxdf(self) -> int:
        return int(self.r.get('rxdf') or 0)
    
    @rxdf.setter
    def rxdf(self, val: int):
        self.r.set('rxdf', val)
        
    @property
    def txdf(self) -> int:
        return int(self.r.get('txdf') or 0)
    
    @txdf.setter
    def txdf(self, val: int):
        self.r.set('txdf', val)
    
    @property
    def last_tx(self) -> str:
        return (self.r.get('last_tx') or b'').decode()
    
    @last_tx.setter
    def last_tx(self, val: str):
        self.r.set('last_tx', val)

    @property
    def tx_even(self) -> bool:
        return not not self.r.get('tx_even')
    
    @tx_even.setter
    def tx_even(self, val: bool):
        self.r.set('tx_even', 1 if val else '')
    # ==========================================================

    # SCRIPT PARAMS
    # Not configurable by user
    # ==========================================================
    @property
    def transmitter_started(self) -> bool:
        return not not self.r.get('transmitter_started')
    
    @transmitter_started.setter
    def transmitter_started(self, val: bool):
        self.r.set('transmitter_started', 1 if val else '')
    
    @property
    def receiver_started(self) -> bool:
        return not not self.r.get('receiver_started')
    
    @receiver_started.setter
    def receiver_started(self, val: bool):
        self.r.set('receiver_started', 1 if val else '')

    @property
    def transmit_phase(self) -> bool:
        return not not self.r.get('transmit_phase')
    
    @transmit_phase.setter
    def transmit_phase(self, val: bool):
        self.r.set('transmit_phase', 1 if val else '')

    @property
    def odd_frequencies(self) -> list:
        return [int(i) for i in self.r.lrange('odd_frequencies', 0, -1)]
    
    @odd_frequencies.setter
    def odd_frequencies(self, val: list):
        self.r.delete('odd_frequencies')
        self.r.rpush('odd_frequencies', *val)

    def add_odd_frequency(self, val: int):
        self.r.rpush('odd_frequencies', val)

    @property
    def even_frequencies(self) -> list:
        return [int(i) for i in self.r.lrange('even_frequencies', 0, -1)]
    
    @even_frequencies.setter
    def even_frequencies(self, val: list):
        self.r.delete('even_frequencies')
        self.r.rpush('even_frequencies', *val)

    def add_even_frequency(self, val: int):
        self.r.rpush('even_frequencies', val)

    @property
    def current_callsign(self) -> str:
        return (self.r.get('current_callsign') or b'').decode()
    
    @current_callsign.setter
    def current_callsign(self, val: str):
        self.r.set('current_callsign', val)

    @property
    def inactive_count(self) -> int:
        return int(self.r.get('inactive_count') or 0)
    
    @inactive_count.setter
    def inactive_count(self, val: int):
        self.r.set('inactive_count', val)
    
    @property
    def tries(self) -> int:
        return int(self.r.get('tries') or 0)
    
    @tries.setter
    def tries(self, val: int):
        self.r.set('tries', val)

    @property
    def transmit_counter(self) -> int:
        return int(self.r.get('transmit_counter') or 0)
    
    @transmit_counter.setter
    def transmit_counter(self, val: int):
        self.r.set('transmit_counter', val)

    @property
    def enable_transmit_counter(self) -> int:
        return int(self.r.get('enable_transmit_counter') or 0)
    
    @enable_transmit_counter.setter
    def enable_transmit_counter(self, val: int):
        self.r.set('enable_transmit_counter', val)
    # ==========================================================

    # CONFIGURABLE PARAMS
    # ==========================================================
    @property
    def num_inactive_before_cut(self) -> int:
        return int(self.r.get('num_inactive_before_cut') or 0)
    
    @num_inactive_before_cut.setter
    def num_inactive_before_cut(self, val: int):
        self.r.set('num_inactive_before_cut', val)

    @property
    def num_disable_transmit(self) -> int:
        return int(self.r.get('num_disable_transmit') or 0)
    
    @num_disable_transmit.setter
    def num_disable_transmit(self, val: int):
        self.r.set('num_disable_transmit', val)

    @property
    def max_tries(self) -> int:
        return int(self.r.get('max_tries') or 0)
    
    @max_tries.setter
    def max_tries(self, val: int):
        self.r.set('max_tries', val)

    @property
    def max_tries_change_freq(self) -> int:
        return int(self.r.get('max_tries_change_freq') or 0)
    
    @max_tries_change_freq.setter
    def max_tries_change_freq(self, val: int):
        self.r.set('max_tries_change_freq', val)

    @property
    def sort_by(self) -> list:
        return [json.loads(s) for s in self.r.lrange('sort_by', 0, -1)]
    
    @sort_by.setter
    def sort_by(self, val: list):
        self.r.delete('sort_by')
        self.r.rpush('sort_by', json.dumps(['importance', -1]))
        self.r.rpush('sort_by', *[json.dumps(l) for l in val])

    @property
    def min_db(self) -> int:
        return int(self.r.get('min_db') or 0)
    
    @min_db.setter
    def min_db(self, val: int):
        self.r.set('min_db', val)

    @property
    def new_grid(self) -> bool:
        return not not self.r.get('new_grid')
    
    @new_grid.setter
    def new_grid(self, val: bool):
        self.r.set('new_grid', 1 if val else '')

    @property
    def new_dxcc(self) -> bool:
        return not not self.r.get('new_dxcc')
    
    @new_dxcc.setter
    def new_dxcc(self, val: bool):
        self.r.set('new_dxcc', 1 if val else '')

    # @property
    # def max_force_reply_when_busy(self) -> int:
    #     return int(self.r.get('max_force_reply_when_busy') or 0)
    
    # @max_force_reply_when_busy.setter
    # def max_force_reply_when_busy(self, val: int):
    #     self.r.set('max_force_reply_when_busy', val)
    # ==========================================================

    def change_states(self, **kwargs):

        with self.r.pipeline() as p:
            for k,val in kwargs.items():
                if isinstance(val, bool):
                    p = p.set(k, 1 if val else '')
                else:
                    p = p.set(k, val)
                p.execute()
    
    def get_states(self, *args: str) -> dict:

        with self.r.pipeline() as p:
            for k in args:
                p = p.get(k)
            
            result = dict(zip(args, p.execute()))
        
        for k,v in result.items():
            k_types = self.states_types.get(k, None)
            if k_types == bool:
                result[k] = not not v
            elif k_types == int:
                result[k] = int(v or 0)
            else:
                result[k] = (v or b'').decode()
        
        return result
    
    def halt_transmit(self, immediately: bool = True):
        packet = wsjtx.WSHaltTx()
        packet.mode = not immediately

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def clear_window(self, window: wsjtx.Window = wsjtx.Window.BAND):
        packet = wsjtx.WSClear()
        packet.Window = window

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def enable_monitoring(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 11

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def enable_transmit(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 9

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def disable_transmit(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 8

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def enable_gridtx(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 10
        packet.SkipGrid = False
        packet.Offset = 0

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def disable_gridtx(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 10
        packet.SkipGrid = True
        packet.Offset = 0

        self.sock.sendto(packet.raw(), (self.ip, self.port))

    def change_frequency(self, TXdf: int):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 10
        packet.Offset = TXdf

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def change_band(self, frequency: int):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 13
        packet.Frequency = frequency

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def change_transmit_sequence(self, kind: TxSequence):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = kind.value

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def reply(self, decoded_message: dict, TXdf: int = None):
        isEven = self.tx_even
        if 0 <= decoded_message['Time']/1000%30 < 15:
            if isEven:
                self.change_transmit_sequence(TxSequence.ODD)
        elif not isEven:
            self.change_transmit_sequence(TxSequence.EVEN)

        if not self.tx_enabled:
            self.enable_transmit()

        packet = wsjtx.WSReply()
        packet.Time = decoded_message['Time']
        packet.SNR = decoded_message['SNR']
        packet.DeltaTime = decoded_message['DeltaTime']
        packet.DeltaFrequency = decoded_message['DeltaFrequency']
        packet.Mode = decoded_message['Mode']
        packet.Message = decoded_message['Message']

        self.sock.sendto(packet.raw(), (self.ip, self.port))

        if isinstance(TXdf, int):
            self.change_frequency(TXdf)
    
    def log_qso(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 5

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def clear_message(self):
        packet = wsjtx.WSEnableTx()
        packet.NewTxMsgIdx = 16

        self.sock.sendto(packet.raw(), (self.ip, self.port))
    
    def close(self):
        packet = wsjtx.WSClose()

        self.sock.sendto(packet.raw(), (self.ip, self.port))