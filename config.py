from pymongo import ASCENDING, DESCENDING
from dotenv import dotenv_values


# Config that can be edited from .env file
# =========================================================================================
CONNECTION_CONFIG = dotenv_values('.env')

WSJTX_IP = CONNECTION_CONFIG['WSJTX_IP']
WSJTX_PORT = int(CONNECTION_CONFIG['WSJTX_PORT'])
MULTICAST = CONNECTION_CONFIG.get('MULTICAST', '').upper() in ['1', 'TRUE', 'YES', 'T','Y']

MONGO_HOST = CONNECTION_CONFIG['MONGO_HOST']
MONGO_PORT = int(CONNECTION_CONFIG['MONGO_PORT'])

REDIS_HOST = CONNECTION_CONFIG['REDIS_HOST']
REDIS_PORT = int(CONNECTION_CONFIG['REDIS_PORT'])

QRZ_API_KEY = CONNECTION_CONFIG.get('QRZ_API_KEY', '')
QRZ_USERNAME = CONNECTION_CONFIG.get('QRZ_USERNAME', '')
QRZ_PASSWORD = CONNECTION_CONFIG.get('QRZ_PASSWORD', '')
# =========================================================================================


# Config that can be edited in this script
# ===================================================================
# Number of days backward to get logs from qrz
# Set to 0 to get all logs
# restart receiver + transmitter
NUM_DAYS_LOG = 0

# Set to True to add message in queue if it's in new grid
# restart receiver + transmitter
NEW_GRID = True

# Set to True to add message in queue if it's in new dxcc
# restart receiver + transmitter
NEW_DXCC = True

# Set to True to add message in queue in log is not confirmed
# restart receiver + transmitter
WORK_ON_UNCONFIRMED_QSO = False

# Number of inactive time of callsign before stop replying
# Set to 0 to disable this feature
# MUST BE LESS THAN MAX_TRIES
# restart receiver + transmitter
NUM_INACTIVE_BEFORE_CUT = 0

# Default max number of tries to reply callsign the same message
# restart receiver + transmitter
MAX_TRIES = 3

# Minimum DB of message to be replied
# restart receiver + transmitter
MIN_DB = -20

# Minimum and Maximum frequency to transmit message
# restart receiver + transmitter
MIN_FREQUENCY = 1500
MAX_FREQUENCY = 2200

# Maximum time callsign in queue in seconds
# Set to 0 will make the callsign in queue indefinitely
 # restart receiver + transmitter
EXPIRED_TIME = 0

# List of DXCC that callsign will not be in queue
# restart receiver + transmitter
DXCC_EXCEPTION = [
    "Indonesia",
    "China",
    "Japan",
    "Republic of Korea"
]

# List of DXCC based on priority
# restart receiver + transmitter
DXCC_WANTED = 'data/wanted_list.txt'

# Will change frequency every this number of time
# Set to 1 will always to change frequency every transmit
# restart transmitter
MAX_TRIES_CHANGE_FREQUENCY = 2

# The sorting of queue based on
# The sorting is always based on importance
# restart transmitter
SORTBY = [
    ['timestamp', DESCENDING]
]

# Only used for adif_parser.py
LOG_LOCATION = 'data/log.adi'

# List of callsign that user want to be blacklisted
# Restarting receiver + transmitter is not required (but recommended)
CALLSIGN_EXCEPTION = 'data/Callsign_Exception.txt'
# ===================================================================





# All below this is only for developer
# #########################################################################
DEBUGGING = False

# DON'T CHANGE THIS
TIMING = {
    'FT4': {
        'half': 7.5,
        'full': 15
    },
    'FT8': {
        'half': 15,
        'full': 30
    }
}

if NUM_INACTIVE_BEFORE_CUT >= MAX_TRIES:
    raise ValueError('MIN_INACTIVE_BEFORE_CUT MUST BE LESS THAN MAX_TRIES')
# #########################################################################