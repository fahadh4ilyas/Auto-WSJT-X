#
# BSD 3-Clause License
#
# Copyright (c) 2021, Fred W6BSD
# All rights reserved.
#
# For more information on the WSJT-X protocol look at the file
# NetworkMessage.hpp located in the wsjt-x source directory
# (src/wsjtx/Network/NetworkMessage.hpp)
#
# I use the camel case names to match the names in WSJT-X
# pylint: disable=invalid-name
#
import re
import struct
import ctypes
import typing

from datetime import datetime
from datetime import timedelta
from enum import Enum

WS_MAGIC = 0xADBCCBDA
WS_SCHEMA = 2
WS_VERSION = '1.1'
WS_REVISION = '1a'
WS_CLIENTID = 'EBLINK'

callsign_regex = r'(?P<callsign>\w+)(|/\w+)'
receiver_regex = r'(?P<to>\w+|...)(|/\w+)'
callsign_regex = r'(?P<callsign>(?P<prefixed_callsign>(?:(?P<prefix>[A-Z0-9]{1,4})/)?(?:\d?[A-Z]{1,2}\d(?:[A-Z]{1,4}|\d{3}|\d{1,3}[A-Z])[A-Z]{0,5}))(?:/(?P<suffix>[A-Z0-9]{1,4}))?(?:/(?P<suffix2>[A-Z0-9]{1,4}))?(?:(?P<suffix3>\-\d{1,3}))?)'
receiver_regex = r'(?P<to>(?P<prefixed_to>(?:(?P<prefix_to>[A-Z0-9]{1,4})/)?(?:(?:\d?[A-Z]{1,2}\d(?:[A-Z]{1,4}|\d{3}|\d{1,3}[A-Z])[A-Z]{0,5})|...))(?:/(?P<suffix_to>[A-Z0-9]{1,4}))?(?:/(?P<suffix2_to>[A-Z0-9]{1,4}))?(?:(?P<suffix3_to>\-\d{1,3}))?)'

call_types: typing.Dict[str, re.Pattern] = {
  "CQ": re.compile(r'^<?CQ>? (?:<?(?P<extra>.*)>? )?<?'+callsign_regex+r'>?(?: <?(?P<grid>[A-Z]{2}[0-9]{2})>?)?'),
  "R73": re.compile(r'^<?'+receiver_regex+r'>? <?'+callsign_regex+r'>? (?P<R73>RRR|R*73)'),
  "GRID": re.compile(r'^<?'+receiver_regex+r'>? <?'+callsign_regex+r'>? <?(?P<grid>[A-Z]{2}[0-9]{2})>?'),
  "SNR": re.compile(r'^<?'+receiver_regex+r'>? <?'+callsign_regex+r'>? (?P<snr>0|[-+]\d+)'),
  "RSNR": re.compile(r'^<?'+receiver_regex+r'>? <?'+callsign_regex+r'>? R(?P<snr>0|[-+]\d+)')
}

# Check the file

class PacketType(Enum):
  HEARTBEAT = 0                 # Out/in
  STATUS = 1                    # Out
  DECODE = 2                    # Out
  CLEAR = 3                     # Out/In
  REPLY = 4                     # In
  QSOLOGGED = 5                 # Out
  CLOSE = 6                     # Out/In
  REPLAY = 7                    # In
  HALTTX = 8                    # In
  FREETEXT = 9                  # In
  WSPRDECODE = 10               # Out
  LOCATION = 11                 # In
  LOGGEDADIF = 12               # Out
  HIGHLIGHTCALLSIGN = 13        # In
  SWITCHCONFIGURATION = 14      # In
  CONFIGURE = 15                # In
  ENABLETX = 16
  ENQUEUEDECODE = 17


class Modifiers(Enum):
  NoModifier = 0x00
  SHIFT = 0x02
  CTRL = 0x04
  ALT = 0x08
  META = 0x10
  KEYPAD = 0x20
  GroupSwitch = 0x40

class Window(Enum):
  BAND = 0
  RX = 1
  BOTH = 2


SHEAD = struct.Struct('!III')

class _WSPacket:

  def __init__(self, pkt=None):
    self._data = {}
    self._index = 0            # Keeps track of where we are in the packet parsing!

    if pkt is None:
      self._packet = ctypes.create_string_buffer(250)
      self._magic_number = WS_MAGIC
      self._schema_version = WS_SCHEMA
      self._packet_type = 0
      self._client_id = WS_CLIENTID
    else:
      self._packet = ctypes.create_string_buffer(pkt)
      self._decode()

  def raw(self):
    self._encode()
    return self._packet[:self._index]

  def _decode(self):
    # in here depending on the Packet Type we create the class to handle the packet!
    magic, schema, pkt_type = SHEAD.unpack_from(self._packet)
    self._index += SHEAD.size
    self._magic_number = magic
    self._schema_version = schema
    self._packet_type = pkt_type
    self._client_id = self._get_string()

  def _encode(self):
    self._index = 0
    SHEAD.pack_into(self._packet, 0, self._magic_number,
                    self._schema_version, self._packet_type.value)
    self._index += SHEAD.size
    self._set_string(self._client_id)

  def __repr__(self):
    sbuf = [str(self.__class__)]
    for key, val in sorted(self._data.items()):
      sbuf.append("{}:{}".format(key, val))
    return ', '.join(sbuf)

  def _get_string(self):
    length = self._get_int32()
    # Empty strings have a length of zero whereas null strings have a
    # length field of 0xffffffff.
    if length == -1:
      return None
    fmt = '!{:d}s'.format(length)
    string, *_ = struct.unpack_from(fmt, self._packet, self._index)
    self._index += length
    return string.decode('utf-8')

  def _set_string(self, string):
    fmt = ''
    if string is None:
      string = b''
      length = -1
    else:
      string = string.encode('utf-8')
      length = len(string)

    fmt = '!i{:d}s'.format(length)
    struct.pack_into(fmt, self._packet, self._index, length, string)
    self._index += struct.calcsize(fmt)

  def _get_datetime(self):
    time_offset = 0
    date_off = self._get_longlong()
    time_off = self._get_uint32()
    time_spec = self._get_byte()
    if time_spec == 2:
      time_offset = self._get_int32()
    return (date_off, time_off, time_spec, time_offset)

  def _get_data(self, fmt):
    data, *_ = struct.unpack_from(fmt, self._packet, self._index)
    self._index += struct.calcsize(fmt)
    return data

  def _set_data(self, fmt, value):
    struct.pack_into(fmt, self._packet, self._index, value)
    self._index += struct.calcsize(fmt)

  def _get_byte(self):
    return self._get_data('!B')

  def _set_byte(self, value):
    self._set_data('!B', value)

  def _get_bool(self):
    return self._get_data('!?')

  def _set_bool(self, value):
    assert isinstance(value, (bool, int)), "Value should be bool or int"
    self._set_data('!?', value)

  def _get_int32(self):
    return self._get_data('!i')

  def _set_int32(self, value):
    self._set_data('!i', value)

  def _get_uint16(self):
    return self._get_data('!H')

  def _set_uint16(self, value):
    assert isinstance(value, int)
    self._set_data('!H', value)

  def _get_uint32(self):
    return self._get_data('!I')

  def _set_uint32(self, value):
    assert isinstance(value, int)
    self._set_data('!I', value)

  def _get_longlong(self):
    return self._get_data('!Q')

  def _set_longlong(self, value):
    assert isinstance(value, int)
    self._set_data('!Q', value)

  def _get_double(self):
    return self._get_data('!d')

  def _set_double(self, value):
    assert isinstance(value, float)
    self._set_data('!d', value)


class WSHeartbeat(_WSPacket):
  """Packet Type 0 Heartbeat (In/Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.HEARTBEAT

  def __repr__(self):
    return "{} - Schema: {} Version: {} Revision: {}".format(
      self.__class__, self.MaxSchema, self.Version, self.Revision)

  def _decode(self):
    super()._decode()
    self._data['MaxSchema'] = self._get_uint32()
    self._data['Version'] = self._get_string()
    self._data['Revision'] = self._get_string()

  def _encode(self):
    super()._encode()
    self._set_uint32(self.MaxSchema)
    self._set_string(self.Version)
    self._set_string(self.Revision)

  @property
  def MaxSchema(self):
    return self._data.get('MaxSchema', WS_SCHEMA)

  @MaxSchema.setter
  def MaxSchema(self, val):
    self._data['MaxSchema'] = int(val)

  @property
  def Version(self):
    return self._data.get('Version', WS_VERSION)
  
  @Version.setter
  def Version(self, val):
    self._data['Version'] = val

  @property
  def Revision(self):
    return self._data.get('Revision', WS_REVISION)
  
  @Revision.setter
  def Revision(self, val):
    self._data['Revision'] = val


class WSStatus(_WSPacket):
  """Packet Type 1 Status  (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.STATUS

  def __repr__(self):
    keys = [
      'Frequency',
      'Mode',
      'DXCall',
      'Report',
      'TXMode',
      'TXEnabled',
      'Transmitting',
      'Decoding',
      'RXdf',
      'TXdf',
      'DeCall',
      'DeGrid',
      'DXGrid',
      'TXWatchDog',
      'SubMode',
      'FastMode',
      'SpecialOPMode',
      'FrequencyTolerance',
      'TRPeriod',
      'ConfigName',
      'LastTxMsg',
      'QSOProgress',
      'TxEven',
      'CQOnly',
      'GenMsg',
      'TxHaltClicked',
      'NotScript'
    ]
    return ("{} - "+" ".join([i+': '+'{}' for i in keys])).format(
      self.__class__,
      self.Frequency,
      self.Mode,
      self.DXCall,
      self.Report,
      self.TXMode,
      self.TXEnabled,
      self.Transmitting,
      self.Decoding,
      self.RXdf,
      self.TXdf,
      self.DeCall,
      self.DeGrid,
      self.DXGrid,
      self.TXWatchdog,
      self.SubMode,
      self.Fastmode,
      self.SpecialOPMode,
      self.FrequencyTolerance,
      self.TRPeriod,
      self.ConfigName,
      self.LastTxMsg,
      self.QSOProgress,
      self.TxEven,
      self.CQOnly,
      self.GenMsg,
      self.TxHaltClicked,
      self.NotScript)

  def _decode(self):
    super()._decode()
    self._data['Frequency'] = self._get_longlong()
    self._data['Mode'] = self._get_string()
    self._data['DXCall'] = self._get_string()
    self._data['Report'] = self._get_string()
    self._data['TXMode'] = self._get_string()
    self._data['TXEnabled'] = self._get_bool()
    self._data['Transmitting'] = self._get_bool()
    self._data['Decoding'] = self._get_bool()
    self._data['RXdf'] = self._get_uint32()
    self._data['TXdf'] = self._get_uint32()
    self._data['DeCall'] = self._get_string()
    self._data['DeGrid'] = self._get_string()
    self._data['DXGrid'] = self._get_string()
    self._data['TXWatchdog'] = self._get_bool()
    self._data['SubMode'] = self._get_string()
    self._data['Fastmode'] = self._get_bool()
    self._data['SpecialOPMode'] = self._get_byte()
    self._data['FrequencyTolerance'] = self._get_uint32()
    self._data['TRPeriod'] = self._get_uint32()
    self._data['ConfigName'] = self._get_string()
    self._data['LastTxMsg'] = self._get_string()
    self._data['QSOProgress'] = self._get_uint32()
    self._data['TxEven'] = self._get_bool()
    self._data['CQOnly'] = self._get_bool()
    self._data['GenMsg'] = self._get_string()
    self._data['TxHaltClicked'] = self._get_bool()
    self._data['NotScript'] = self._get_bool()

  @property
  def Frequency(self) -> int:
    return self._data['Frequency']

  @property
  def Mode(self) -> str:
    return self._data['Mode']

  @property
  def DXCall(self) -> typing.Optional[str]:
    return self._data['DXCall']

  @property
  def Report(self) -> str:
    return self._data['Report']

  @property
  def TXMode(self) -> str:
    return self._data['TXMode']

  @property
  def TXEnabled(self) -> bool:
    return self._data['TXEnabled']

  @property
  def Transmitting(self) -> bool:
    return self._data['Transmitting']

  @property
  def Decoding(self) -> bool:
    return self._data['Decoding']

  @property
  def RXdf(self) -> int:
    return self._data['RXdf']

  @property
  def TXdf(self) -> int:
    return self._data['TXdf']

  @property
  def DeCall(self) -> typing.Optional[str]:
    return self._data['DeCall']

  @property
  def DeGrid(self) -> typing.Optional[str]:
    return self._data['DeGrid']

  @property
  def DXGrid(self) -> typing.Optional[str]:
    return self._data['DXGrid']

  @property
  def TXWatchdog(self) -> bool:
    return self._data['TXWatchdog']

  @property
  def SubMode(self) -> typing.Optional[str]:
    return self._data['SubMode']

  @property
  def Fastmode(self) -> bool:
    return self._data['Fastmode']

  @property
  def SpecialOPMode(self) -> bytes:
    return self._data['SpecialOPMode']

  @property
  def FrequencyTolerance(self) -> int:
    return self._data['FrequencyTolerance']

  @property
  def TRPeriod(self) -> int:
    return self._data['TRPeriod']

  @property
  def ConfigName(self) -> str:
    return self._data['ConfigName']

  @property
  def LastTxMsg(self) -> typing.Optional[str]:
    return self._data['LastTxMsg']

  @property
  def QSOProgress(self) -> int:
    return self._data['QSOProgress']

  @property
  def TxEven(self) -> bool:
    return self._data['TxEven']

  @property
  def CQOnly(self) -> bool:
    return self._data['CQOnly']

  @property
  def GenMsg(self) -> typing.Optional[str]:
    return self._data['GenMsg']

  @property
  def TxHaltClicked(self) -> bool:
    return self._data['TxHaltClicked']

  @property
  def NotScript(self) -> bool:
    return self._data['NotScript']


class WSDecode(_WSPacket):
  """Packet Type 2  Decode  (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.DECODE

  def _decode(self):
    super()._decode()
    self._data['New'] = self._get_bool()
    self._data['Time'] = self._get_uint32()
    self._data['SNR'] = self._get_int32()
    self._data['DeltaTime'] = round(self._get_double(), 3)
    self._data['DeltaFrequency'] = self._get_uint32()
    self._data['Mode'] = self._get_string()
    self._data['Message'] = self._get_string()
    self._data['LowConfidence'] = self._get_bool()
    self._data['OffAir'] = self._get_bool()

  def __repr__(self):
    keys = [
      'New',
      'Time',
      'SNR',
      'DeltaTime',
      'DeltaFrequency',
      'Mode',
      'Message',
      'LowConfidence',
      'OffAir'
    ]
    return ("{} - "+" ".join([i+': '+'{}' for i in keys])).format(
      self.__class__,
      self.New,
      self.Time,
      self.SNR,
      self.DeltaTime,
      self.DeltaFrequency,
      self.Mode,
      self.Message,
      self.LowConfidence,
      self.OffAir)

  def as_dict(self):
    return self._data

  @property
  def New(self) -> bool:
    return self._data['New']

  @property
  def Time(self) -> int:
    return self._data['Time']

  @property
  def SNR(self) -> int:
    return self._data['SNR']

  @property
  def DeltaTime(self) -> float:
    return self._data['DeltaTime']

  @property
  def DeltaFrequency(self) -> int:
    return self._data['DeltaFrequency']

  @property
  def Mode(self) -> str:
    return self._data['Mode']

  @property
  def Message(self) -> str:
    return self._data['Message']

  @property
  def LowConfidence(self) -> bool:
    return self._data['LowConfidence']

  @property
  def OffAir(self) -> bool:
    return self._data['OffAir']


class WSClear(_WSPacket):
  """Packet Type 3  Clear (Out/In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.CLEAR

  def __repr__(self):
    return "{} - Window: {}".format(
      self.__class__, self.Window
    )

  def _encode(self):
    super()._encode()
    self._set_byte(self._data.get('Window', Window.BAND.value))

  def _decode(self):
    super()._decode()
    self._data['Window'] = self._get_byte()

  @property
  def Window(self):
    return self._data['Window']

  @Window.setter
  def Window(self, val):
    assert isinstance(val, Window)
    self._data['Window'] = val.value


class WSReply(_WSPacket):
  """
  Packet Type 4 Reply (In)
  * Id (target unique key) utf8
  * Time                   quint  (QTime)
  * snr                    qint32
  * Delta time (S)         float (serialized as double)
  * Delta frequency (Hz)   quint32
  * Mode                   utf8
  * Message                utf8
  * Low confidence         bool
  * Modifiers              quint8
  * Not Script             bool
  """

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.REPLY
    self._client_id = "AUTOFT"

  def _encode(self):
    super()._encode()
    self._set_uint32(self.Time)
    self._set_int32(self.SNR)
    self._set_double(self.DeltaTime)
    self._set_uint32(self.DeltaFrequency)
    self._set_string(self.Mode)
    self._set_string(self.Message)
    self._set_bool(self._data.get('LowConfidence', False))
    self._set_byte(self._data.get('Modifiers', Modifiers.NoModifier.value))
    self._set_bool(self._data.get('NotScript', True))

  @property
  def Time(self):
    return self._data.get('Time')

  @Time.setter
  def Time(self, val):
    assert isinstance(val, int), 'Object datetime expected'
    self._data['Time'] = val

  @property
  def SNR(self):
    return self._data.get('SNR')

  @SNR.setter
  def SNR(self, val):
    self._data['SNR'] = int(val)

  @property
  def DeltaTime(self):
    return self._data.get('DeltaTime')

  @DeltaTime.setter
  def DeltaTime(self, val):
    self._data['DeltaTime'] = float(val)

  @property
  def DeltaFrequency(self):
    return self._data.get('DeltaFrequency')

  @DeltaFrequency.setter
  def DeltaFrequency(self, val):
    self._data['DeltaFrequency'] = int(val)

  @property
  def Mode(self):
    return self._data.get('Mode')

  @Mode.setter
  def Mode(self, val):
    self._data['Mode'] = val

  @property
  def Message(self):
    return self._data.get('Message')

  @Message.setter
  def Message(self, val):
    self._data['Message'] = val

  @property
  def LowConfidence(self):
    return self._data.get('LowConfidence')

  @LowConfidence.setter
  def LowConfidence(self, val):
    self._data['LowConfidence'] = bool(val)

  @property
  def Modifiers(self):
    return self._data.get('Modifiers', Modifiers.NoModifier)

  @Modifiers.setter
  def Modifiers(self, modifier):
    assert isinstance(modifier, Modifiers)
    self._data['Modifiers'] = modifier.value

  @property
  def NotScript(self):
    return self._data.get('NotScript')

  @NotScript.setter
  def NotScript(self, val):
    self._data['NotScript'] = bool(val)


class WSLogged(_WSPacket):
  """Packet Type 5 QSO Logged (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.QSOLOGGED

  def _decode(self):
    super()._decode()
    dt_tuple = self._get_datetime()
    self._data['DateOff'] = dt_tuple[0]
    self._data['TimeOff'] = dt_tuple[1]
    self._data['TimeOffSpec'] = dt_tuple[2]
    self._data['TimeOffOffset'] = dt_tuple[3]
    self._data['DXCall'] = self._get_string()
    self._data['DXGrid'] = self._get_string()
    self._data['DialFrequency'] = self._get_longlong()
    self._data['Mode'] = self._get_string()
    self._data['ReportSent'] = self._get_string()
    self._data['ReportReceived'] = self._get_string()
    self._data['TXPower'] = self._get_string()
    self._data['Comments'] = self._get_string()
    self._data['Name'] = self._get_string()
    dt_tuple = self._get_datetime()
    self._data['DateOn'] = dt_tuple[0]
    self._data['TimeOn'] = dt_tuple[1]
    self._data['TimeOnSpec'] = dt_tuple[2]
    self._data['TimeOnOffset'] = dt_tuple[3]

  @property
  def DateOff(self):
    return self._data['DateOff']

  @property
  def TimeOff(self):
    return self._data['TimeOff']

  @property
  def TimeOffSpec(self):
    return self._data['TimeOffSpec']

  @property
  def TimeOffOffset(self):
    return self._data['TimeOffOffset']

  @property
  def DXCall(self):
    return self._data['DXCall']

  @property
  def DXGrid(self):
    return self._data['DXGrid']

  @property
  def DialFrequency(self):
    return self._data['DialFrequency']

  @property
  def Mode(self):
    return self._data['Mode']

  @property
  def ReportSent(self):
    return self._data['ReportSent']

  @property
  def ReportReceived(self):
    return self._data['ReportReceived']

  @property
  def TXPower(self):
    return self._data['TXPower']

  @property
  def Comments(self):
    return self._data['Comments']

  @property
  def Name(self):
    return self._data['Name']

  @property
  def DateOn(self):
    return self._data['DateOn']

  @property
  def TimeOn(self):
    return self._data['TimeOn']

  @property
  def TimeOnSpec(self):
    return self._data['TimeOnSpec']

  @property
  def TimeOnOffset(self):
    return self._data['TimeOnOffset']


class WSClose(_WSPacket):
  """Packet Type 6 Close (Out/In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.CLOSE
  
  def __repr__(self):
    return "{}".format(self.__class__)

class WSReplay(_WSPacket):
  """Packet Type 7 Replay (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.REPLAY


class WSHaltTx(_WSPacket):
  """Packet Type 8 Halt Tx (In)
  self.mode = True
      Will stop the transmission at the end of the sequency
  self.mode = False
      Will stop the transmission immediately
  """

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.HALTTX
    self._data['mode'] = False

  def _encode(self):
    super()._encode()
    self._set_bool(self._data['mode'])

  @property
  def mode(self):
    return self._data['mode']

  @mode.setter
  def mode(self, val):
    assert isinstance(val, bool)
    self._data['mode'] = val


class WSFreeText(_WSPacket):
  """Packet Type 9 Free Text (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.FREETEXT

  def _encode(self):
    super()._encode()
    self._set_string(self._data.get('text', ''))
    self._set_bool(self._data.get('send', True))

  @property
  def text(self):
    return self._data.get('text', '')

  @text.setter
  def text(self, val):
    assert isinstance(val, str), 'Expecting a string'
    self._data['text'] = val

  @property
  def send(self):
    return self._data.get('send', True)

  @send.setter
  def send(self, val):
    assert isinstance(val, bool), 'Expecting a boolean'
    self._data['send'] = val


class WSWSPRDecode(_WSPacket):
  """Packet Type 10 WSPR Decode (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.WSPRDECODE

class WSLocation(_WSPacket):
  """Packet Type 11 Location (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.LOCATION

class WSADIF(_WSPacket):
  """Packet Type 12 Logged ADIF (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.LOGGEDADIF

  def _decode(self):
    super()._decode()
    self._data['ADIF'] = self._get_string()

  def __str__(self):
    return ''.join(self._data['ADIF'].split('\n'))

  def __repr__(self):
    if 'ADIF' in self._data:
      return "{} {}".format(self.__class__, self._data['ADIF'])
    return "{} {}".format(self.__class__, self._packet.raw)

  @property
  def Id(self):
    return self._data['Id']

  @property
  def ADIF(self):
    return self._data['ADIF']


class WSHighlightCallsign(_WSPacket):
  """
  Packet Type 13 Highlight Callsign (In)
  Callsign               utf8
  Background Color       QColor
  Foreground Color       QColor
  Highlight last         bool
  """

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.HIGHLIGHTCALLSIGN

  def _encode(self):
    super()._encode()
    self._set_string(self._data['call'])

    self._set_uint16(0xffff)
    for val in self._data.get('Foreground', (0xffff, 0xff, 0xff)):
      self._set_uint16(val)

    self._set_uint16(0xffff)
    for val in self._data.get('Background', (0, 0, 0)):
      self._set_uint16(val)

    self._set_bool(self._data.get('HighlightLast', True))

  def __repr__(self):
    return "{} call: {}".format(self.__class__, self._data.get('call', 'NoCall'))

  @property
  def call(self):
    return self._data['call']

  @call.setter
  def call(self, call):
    assert isinstance(call, str), 'The callsign must be a string'
    self._data['call'] = call

  @property
  def Background(self):
    return self._data['Background']

  @Background.setter
  def Background(self, rgb):
    assert isinstance(rgb, (list, tuple)), "Tuple object expected"
    self._data['Background'] = rgb

  @property
  def Foreground(self):
    return self._data['Foreground']

  @Foreground.setter
  def Foreground(self, rgb):
    assert isinstance(rgb, (list, tuple)), "Tuple object expected"
    self._data['Foreground'] = rgb

  @property
  def HighlightLast(self):
    return self._data['HighlightLast']

  @HighlightLast.setter
  def HighlightLast(self, val):
    self._data['HighlightLast'] = bool(val)


class WSSwitchConfiguration(_WSPacket):
  """Packet Type 14 Switch Configuration (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.SWITCHCONFIGURATION


class WSConfigure(_WSPacket):
  """Packet Type 15 Configure (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.CONFIGURE

class WSEnableTx(_WSPacket):
  """Packet Type 16 Enable Tx (In)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.ENABLETX

  def _encode(self):
    super()._encode()
    self._set_uint32(self.NewTxMsgIdx)
    self._set_string(self._data.get('GenMsg',''))
    self._set_bool(self._data.get('SkipGrid', True))
    self._set_bool(self._data.get('UseRR73', True))
    self._set_string(self._data.get('CmdCheck', ''))
    self._set_uint32(self._data.get('Offset', 200))
    self._set_longlong(self._data.get('Frequency', 0))
  
  @property
  def NewTxMsgIdx(self):
    return self._data['NewTxMsgIdx']

  @NewTxMsgIdx.setter
  def NewTxMsgIdx(self, val):
    self._data['NewTxMsgIdx'] = int(val)

  @property
  def GenMsg(self):
    return self._data['GenMsg']

  @GenMsg.setter
  def GenMsg(self, val):
    self._data['GenMsg'] = val

  @property
  def SkipGrid(self):
    return self._data['SkipGrid']

  @SkipGrid.setter
  def SkipGrid(self, val):
    self._data['SkipGrid'] = bool(val)
  
  @property
  def UseRR73(self):
    return self._data['UseRR73']

  @UseRR73.setter
  def UseRR73(self, val):
    self._data['UseRR73'] = bool(val)
  
  @property
  def CmdCheck(self):
    return self._data['CmdCheck']

  @CmdCheck.setter
  def CmdCheck(self, val):
    self._data['CmdCheck'] = val

  @property
  def Offset(self):
    return self._data['Offset']

  @Offset.setter
  def Offset(self, val):
    self._data['Offset'] = int(val)

  @property
  def Frequency(self):
    return self._data['Frequency']

  @Frequency.setter
  def Frequency(self, val):
    self._data['Frequency'] = int(val)

class WSEnqueueDecode(_WSPacket):
  """Packet Type 17 Enqueue  Decode  (Out)"""

  def __init__(self, pkt=None):
    super().__init__(pkt)
    self._packet_type = PacketType.ENQUEUEDECODE

  def _decode(self):
    super()._decode()
    self._data['AutoGen'] = self._get_bool()
    self._data['Time'] = self._get_uint32()
    self._data['SNR'] = self._get_int32()
    self._data['DeltaTime'] = round(self._get_double(), 3)
    self._data['DeltaFrequency'] = self._get_uint32()
    self._data['Mode'] = self._get_string()
    self._data['Message'] = self._get_string()
    self._data['IsDX'] = self._get_bool()
    self._data['Modifier'] = self._get_bool()

  def __repr__(self):
    keys = [
      'AutoGen',
      'Time',
      'SNR',
      'DeltaTime',
      'DeltaFrequency',
      'Mode',
      'Message',
      'IsDX',
      'Modifier'
    ]
    return ("{} - "+" ".join([i+': '+'{}' for i in keys])).format(
      self.__class__,
      self.AutoGen,
      self.Time,
      self.SNR,
      self.DeltaTime,
      self.DeltaFrequency,
      self.Mode,
      self.Message,
      self.IsDX,
      self.Modifier)

  def as_dict(self):
    return self._data

  @property
  def AutoGen(self) -> bool:
    return self._data['AutoGen']

  @property
  def Time(self) -> int:
    return self._data['Time']

  @property
  def SNR(self) -> int:
    return self._data['SNR']

  @property
  def DeltaTime(self) -> float:
    return self._data['DeltaTime']

  @property
  def DeltaFrequency(self) -> int:
    return self._data['DeltaFrequency']

  @property
  def Mode(self) -> str:
    return self._data['Mode']

  @property
  def Message(self) -> str:
    return self._data['Message']

  @property
  def IsDX(self) -> bool:
    return self._data['IsDX']

  @property
  def Modifier(self) -> bool:
    return self._data['Modifier']

def wstime2datetime(qtm):
  """wsjtx time containd the number of milliseconds since midnight"""
  tday_midnight = datetime.combine(datetime.utcnow(), datetime.min.time())
  return tday_midnight + timedelta(milliseconds=qtm)

def datetime2wstime(dtime):
  """wsjtx time containd the number of milliseconds since midnight"""
  tday_midnight = datetime.combine(datetime.utcnow(), datetime.min.time())
  return int((dtime - tday_midnight).total_seconds() * 1000)

def ft8_decode(pkt):
  """Look at the packets header and return a class corresponding to the packet"""
  magic, _, pkt_type = SHEAD.unpack_from(pkt)
  if magic != WS_MAGIC:
    raise IOError('Not a WSJT-X packet')

  switch_map = {
    PacketType.HEARTBEAT.value: WSHeartbeat,
    PacketType.STATUS.value: WSStatus,
    PacketType.DECODE.value: WSDecode,
    PacketType.CLEAR.value: WSClear,
    PacketType.REPLY.value: WSReply,
    PacketType.QSOLOGGED.value: WSLogged,
    PacketType.CLOSE.value: WSClose,
    PacketType.LOGGEDADIF.value: WSADIF,
    PacketType.HIGHLIGHTCALLSIGN.value: WSHighlightCallsign,
    PacketType.ENQUEUEDECODE.value: WSEnqueueDecode
  }

  try:
    return switch_map[pkt_type](pkt)
  except KeyError:
    raise NotImplementedError("Packet type '{:d}' unknown".format(pkt_type)) from None
