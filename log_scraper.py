import requests, re, typing
from tqdm import tqdm
from bs4 import BeautifulSoup as soup
from datetime import datetime as dt, date
from multiprocessing import Pool

class OneAdifGetter:
    
    def __init__(self, session: requests.Session, bookid: int, ):
        self.session = session
        self.bookid = bookid
        self.getpages = {'op': 'show', 'bookid': bookid}
        
    def __call__(self, pos: int = 0) -> str:
        try:
            r = self.session.post('http://logbook.qrz.com', data={'logpos': pos, **self.getpages})
            data = soup(r.text, features="lxml")
            table = data.select_one('div#lbrecord table')
            result = {}
            border = dict([('Mode', 'Frequency'), ('RST Rcvd', 'Power'), ('Distance', 'Grid'), ('IOTA', 'Continent')])
            for tr in table.select('tr'):
                for th in tr.select('th'):
                    th_text = th.text.strip()
                    result[th_text] = result.get(th_text, [])
                    td = th.find_next_sibling()
                    while td and td.name == 'td':
                        result[th_text].append(td.text.strip())
                        td = td.find_next_sibling()
                    if th_text in border:
                        latest_td = result[th_text].pop(-1)
                        result[border[th_text]].append(latest_td)
                        border.pop(th_text)
            adif_result = ''
            if 'QSO Start' in result:
                start_dt = dt.strptime(f'{result["QSO Start"][0]}', '%Y-%m-%d %H:%M:%S UTC')
                adif_result = adif_result + f'<QSO_DATE:8>{start_dt.strftime("%Y%m%d")} <TIME_ON:4>{start_dt.strftime("%H%M")} '
            if 'QSO End' in result:
                end_dt = dt.strptime(f'{result["QSO End"][0]}', '%Y-%m-%d %H:%M:%S UTC')
                adif_result = adif_result + f'<QSO_DATE_OFF:8>{end_dt.strftime("%Y%m%d")} <TIME_OFF:4>{end_dt.strftime("%H%M")} '
            if 'Station' in result:
                callsign = result['Station'][0]
                adif_result = adif_result + f'<CALL:{len(callsign)}>{callsign} '
            if 'Country (DXCC)' in result:
                country = result['Country (DXCC)'][0]
                if country:
                    adif_result = adif_result + f'<COUNTRY:{len(country.encode())}>{country} '
            if 'Frequency' in result:
                frequency = result['Frequency'][1].split()[0]
                adif_result = adif_result + f'<FREQ:{len(frequency)}>{frequency} '
                if len(result['Frequency'][1].splitlines()) > 1:
                    band = result['Frequency'][1].split()[-1]
                    adif_result = adif_result + f'<BAND:{len(band)}>{band} '
            if 'Mode' in result:
                mode = result['Mode'][1]
                adif_result = adif_result + f'<MODE:{len(mode)}>{mode} '
            if 'Continent' in result:
                continent = result['Continent'][0].split()[0]
                if continent:
                    adif_result = adif_result + f'<CONT:{len(continent)}>{continent} '
            if 'Grid' in result:
                grid = result['Grid'][0]
                if grid:
                    adif_result = adif_result + f'<GRIDSQUARE:{len(grid)}>{grid} '
            if 'Confirmed' in result:
                confirmed = result['Confirmed'][0]
                if confirmed in ['no', '']:
                    adif_result = adif_result + '<APP_QRZLOG_STATUS:1>N '
                else:
                    adif_result = adif_result + '<APP_QRZLOG_STATUS:1>C '
            else:
                adif_result = adif_result + '<APP_QRZLOG_STATUS:1>N '
            if 'Status' in result:
                lotw_rec, lotw_sen = result['Status'][2], result['Status'][3]
                if lotw_rec in ['None', 'none', '']:
                    adif_result = adif_result + '<LOTW_QSL_RCVD:1>N '
                else:
                    adif_result = adif_result + '<LOTW_QSL_RCVD:1>Y '
                if lotw_sen in ['None', 'none', '']:
                    adif_result = adif_result + '<LOTW_QSL_SENT:1>N '
                else:
                    adif_result = adif_result + '<LOTW_QSL_SENT:1>Y '
            else:
                adif_result = adif_result + '<LOTW_QSL_RCVD:1>N <LOTW_QSL_SENT:1>N '
            adif_result = adif_result + '<EOR>'
            return adif_result
        except:
            return ''

def main(username: str, password: str, min_date: date = None) -> typing.List[str]:
    
    s = requests.Session()
    
    payload = {
        'username': username,
        'password': password
    }
    
    res = s.post('https://www.qrz.com/login', data=payload)
    
    if not res.ok:
        raise ValueError('Can not login!')
    
    r = s.post('http://logbook.qrz.com', data={'page':1})
    
    data = soup(r.text, features="lxml")
    bookids = []
    all_bookids = data.findAll('option', attrs={'id':re.compile('^booksel'),'value':re.compile('^[0-9]+$')})
    for bookid in all_bookids:
        bookids.append(int(bookid['value']))
    
    log_list = []
    for bookid in bookids:
        print(f'Start for book ID {bookid}')
        if min_date is None:
            r = s.post('http://logbook.qrz.com', data={'bookid':bookid})
            data = soup(r.text, features="lxml")
            total_qsos = data.select_one('input#logcount')
            if total_qsos is None:
                print(f'No QSO found in book ID {bookid}')
                continue
            log_count = int(total_qsos['value'])
        else:
            page = 1
            r = s.post('http://logbook.qrz.com', data={'bookid': bookid, 'page': page})
            data = soup(r.text, features="lxml")
            found = False
            log_count = 0
            while int((data.select_one('input#ipage') or {'value': 0})['value']) == page and not found:
                table = data.select_one('table#lbtab')
                for tr in table.tbody.select('tr'):
                    td_date = tr.select_one('td.td_date')
                    if td_date is None:
                        continue
                    data_date = dt.strptime(td_date.text, '%Y-%m-%d').date()
                    if data_date < min_date:
                        log_count = int(tr['data-pos'])
                        found = True
                        break
                if not found:
                    page += 1
                    r = s.post('http://logbook.qrz.com', data={'bookid': bookid, 'page': page})
                    data = soup(r.text, features="lxml")
            if not log_count:
                r = s.post('http://logbook.qrz.com', data={'bookid':bookid})
                data = soup(r.text, features="lxml")
                total_qsos = data.select_one('input#logcount')
                if total_qsos is None:
                    print(f'No QSO found in book ID {bookid}')
                    continue
                log_count = int(total_qsos['value'])
        print(f'Number of log: {log_count}')
        engine = OneAdifGetter(s, bookid)
        pool = Pool()
        log_list.extend([i for i in tqdm(pool.imap(engine, range(log_count)), total=log_count) if i])
    return log_list

if __name__ == '__main__':
    from config import QRZ_USERNAME, QRZ_PASSWORD
    from adif_parser import main as adif_parser, done_coll
    
    if QRZ_USERNAME and QRZ_PASSWORD:
        print('Start scraping...')
        result_adif_list = main(QRZ_USERNAME, QRZ_PASSWORD)
        print('DONE!')
        print('Emptying the database...')
        done_coll.delete_many({})
        print('DONE!')
        print('Start putting to database...')
        if result_adif_list:
            adif_parser('\n'.join(result_adif_list))
        print('DONE!')