import json, adif_io, requests, re, typing
from pyhamtools import LookupLib, Callinfo
from pyhamtools.frequency import freq_to_band
from tqdm import tqdm

from pymongo import MongoClient
from config import LOG_LOCATION, MONGO_HOST, MONGO_PORT, QRZ_API_KEY, QRZ_PASSWORD, QRZ_USERNAME

call_info2 = None
if QRZ_USERNAME:
    lookup_lib2 = LookupLib(lookuptype='qrz', username=QRZ_USERNAME, pwd=QRZ_PASSWORD)
    call_info2 = Callinfo(lookup_lib2)
lookup_lib = LookupLib(filename='data/cty.plist')
call_info = Callinfo(lookup_lib)

with open('data/countrytodxcc.json') as f:
    country_to_dxcc: dict = json.load(f)

mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)

db = mongo_client.wsjt
done_coll = db.black

def string_band_to_number(band: str) -> typing.Union[float, int]:
    if 'mm' in band.lower():
        band = band[:-2]
        band.replace(',','.')
        band = float(band)/1000
    elif 'cm' in band.lower():
        band = band[:-2]
        band.replace(',','.')
        band = float(band)/100
    elif 'm' in band.lower():
        band = band[:-1]
        band = int(band)
    
    return band

def is_confirmed(data: dict) -> bool:

    if data.get('APP_QRZLOG_STATUS', 'C') == 'C':
        return True
    if data.get('LOTW_QSL_SENT', 'N') == 'Y' and data.get('LOTW_QSL_RCVD', 'N') == 'Y':
        return True
    
    return False

def main(data_str: str, updating: bool = False):

    t = adif_io.read_from_string(data_str)

    data, _ = typing.cast(
        typing.Tuple[
            typing.List[typing.Dict[str, typing.Any]],
            typing.Dict[str, typing.Any]
        ],
        t
    )

    for d in tqdm(data):
        if d.get('MODE', None) not in ['FT8', 'FT4']:
            continue

        inserted_data = {
            'callsign': d['CALL'].replace('_', '/'),
            'mode': d.get('MODE', 'FT8'),
            'confirmed': is_confirmed(d)
        }

        try:
            inserted_data['band'] = freq_to_band(d['FREQ']*1000)['band']
        except:
            inserted_data['band'] = string_band_to_number(d['BAND'])
            
        if 'GRIDSQUARE' in d:
            inserted_data['grid'] = d['GRIDSQUARE']
        try:
            location_data = call_info.get_all(d['CALL'])
        except:
            if call_info2:
                try:
                    location_data = call_info2.get_all(d['CALL'])
                except:
                    location_data = {}

        if 'COUNTRY' in d:
            inserted_data['country'] = d['COUNTRY']
        elif location_data:
            inserted_data['country'] = location_data['country']

        if inserted_data.get('country', None) == 'United States':
            state_data = {}

            if 'STATE' in d:
                inserted_data['state'] = d['STATE']
            elif call_info2:
                try:
                    state_data = call_info2.get_all(d['CALL'])
                except:
                    pass
                if 'state' in state_data:
                    inserted_data['state'] = state_data['state']

            if 'CNTY' in d:
                inserted_data['county'] = d['CNTY'][3:]
            elif 'county' in state_data:
                inserted_data['county'] = state_data['county']

        if 'CONT' in d:
            inserted_data['continent'] = d['CONT']
        elif location_data:
            inserted_data['continent'] = location_data['continent']
        elif inserted_data.get('country', None) == 'United States':
            inserted_data['continent'] = 'NA'

        if 'DXCC' in d:
            inserted_data['dxcc'] = int(d['DXCC'])
        elif inserted_data['country'] and inserted_data['country'] in country_to_dxcc:
            inserted_data['dxcc'] = country_to_dxcc[inserted_data['country']]

        if 'DISTANCE' in d:
            inserted_data['distance'] = float(d['DISTANCE'])
        
        if updating:
            done_coll.update_one({'callsign': inserted_data['callsign'], 'band': inserted_data['band']}, {'$set': inserted_data}, upsert=True)
        else:
            done_coll.insert_one({'callsign': inserted_data['callsign'], 'band': inserted_data['band']}, {inserted_data})

if __name__ == '__main__':
    print('Starting...')
    if QRZ_API_KEY:
        res = requests.post('https://logbook.qrz.com/api',data=f'KEY={QRZ_API_KEY}&ACTION=FETCH')
        if res.ok:
            result_str = res.text.replace('&lt;','<').replace('&gt;','>').replace('\n', ' ')
            result_adif = re.search(r'ADIF=(.*<eor>)', result_str)
            if result_adif:
                data_str = result_adif.group(1)
                main(data_str)
    elif LOG_LOCATION:
        with open(LOG_LOCATION, encoding='latin-1') as f:
            data_str = f.read()
        main(data_str)
    print('DONE!')