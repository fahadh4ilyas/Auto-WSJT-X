import os
from pymongo import ASCENDING, DESCENDING
from dotenv import dotenv_values

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Config that can be edited from .env file
# =========================================================================================
CONNECTION_CONFIG = dotenv_values(os.path.join(CURRENT_DIR, '.env'))

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

# Set to True to make grid message higher importance than CQ
# restart receiver + transmitter
GRID_HIGHER_THAN_CQ = True

# Set to True to validate callsign first before adding to queue
# restart receiver + transmitter
VALIDATE_CALLSIGN = True

# Set to True to exclude unconfirmed qso from blacklist
# If this True, EXCLUDE_UNCONFIRMED_QSO_DATE_RANGE will be ignored
# restart receiver + transmitter
WORK_ON_UNCONFIRMED_QSO = False

# Set the date where unconfirmed qso is excluded from blacklist
# The date format is YYYY-MM-DD
# If the value is non-negative integer,
# will be interpret as today minus that number of days
# Both end of date is affected
# Set None to 'from' means from beginning of time
# Set None to 'to' means until today
# Set None to both means don't apply this configuration
EXCLUDE_UNCONFIRMED_QSO_DATE_RANGE = {
    'from': None,
    'to': None
}

# Number of inactive time of callsign before stop replying
# Set to 0 to disable this feature
# MUST BE LESS THAN MAX_TRIES
# restart receiver + transmitter
NUM_INACTIVE_BEFORE_CUT = 0

# Default max number of tries to reply callsign the same message
# restart receiver + transmitter
MAX_TRIES = 3

# Number of tries calling busy callsign
# Minimum must be set to 0
# Maximum is 2 * MAX_TRIES - 1
# restart receiver + transmitter
NUM_TRIES_CALL_BUSY = 2

# Disable transmit button every number of tims
# set to 0 means disable only when no message queue
# set to 1 means always disable after every transmit
# restart receiver + transmitter
NUM_DISABLE_TRANSMIT = 1

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

# Maximum time callsign will be in spam
# Set to 0 will make the callsign in spam indefinitely
# Note: restarting receiver will remove callsign from spam
# restart receiver + transmitter
RELEASE_FROM_SPAM_TIME = 0

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
DXCC_PRIORITY = os.path.join(CURRENT_DIR, 'data', 'priority_list.txt')

# List of VIP DXCC
# restart receiver + transmitter
DXCC_VIP = os.path.join(CURRENT_DIR, 'data', 'vip_list.txt')

# Number of inactive time of callsign before stop replying
# This is for VIP DXCC
# Set to 0 to disable this feature
# MUST BE LESS THAN MAX_TRIES
# restart receiver + transmitter
NUM_INACTIVE_BEFORE_CUT_VIP = 0

# Default max number of tries to reply callsign the same message
# This is for VIP DXCC
# restart receiver + transmitter
MAX_TRIES_VIP = 3

# Number of tries calling busy callsign
# This is for VIP DXCC
# Minimum must be set to 0
# Maximum is 2 * MAX_TRIES - 1
# restart receiver + transmitter
NUM_TRIES_CALL_BUSY_VIP = 2

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
LOG_LOCATION = os.path.join(CURRENT_DIR, 'data', 'log.adi')

# List of valid callsign based on lotw
VALID_CALLSIGN_LOCATION = os.path.join(CURRENT_DIR, 'data', 'lotw-user-activity.csv')

# List of receiver callsign that user want to be blacklisted
# if that callsign is being called by callsign that we wanted
# Restarting receiver + transmitter is not required (but recommended)
RECEIVER_EXCEPTION = os.path.join(CURRENT_DIR, 'data', 'Receiver_Exception.txt')

# List of callsign that user want to be blacklisted
# Restarting receiver + transmitter is not required (but recommended)
CALLSIGN_EXCEPTION = os.path.join(CURRENT_DIR, 'data', 'Callsign_Exception.txt')
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
    raise ValueError('MIN_INACTIVE_BEFORE_CUT must be less than MAX_TRIES')

if not (0 <= NUM_TRIES_CALL_BUSY < 2*MAX_TRIES):
    raise ValueError('NUM_TRIES_CALL_BUSY must be less than 2 * MAX_TRIES')

if NUM_INACTIVE_BEFORE_CUT_VIP >= MAX_TRIES_VIP:
    raise ValueError('MIN_INACTIVE_BEFORE_CUT_VIP must be less than MAX_TRIES_VIP')

if not (0 <= NUM_TRIES_CALL_BUSY_VIP < 2*MAX_TRIES_VIP):
    raise ValueError('NUM_TRIES_CALL_BUSY_VIP must be less than 2 * MAX_TRIES_VIP')
# #########################################################################