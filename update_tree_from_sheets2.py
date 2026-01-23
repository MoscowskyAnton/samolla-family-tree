import json
import re
from collections import defaultdict, OrderedDict
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from googleapiclient.http import MediaIoBaseDownload
import io
import os

def extract_drive_file_id(url):
    match = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None

def rus_to_translit(text):
    translit_dict = {
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E', 'Ж': 'Zh',
        'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O',
        'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts',
        'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch', 'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu',
        'Я': 'Ya',
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e', 'ж': 'zh',
        'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
        'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
        'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
        'я': 'ya'
    }
    return ''.join(translit_dict.get(c, c) for c in text)

# Конфигурация
SERVICE_ACCOUNT_FILE = 'samolla-b4398f9d675c.json'
SPREADSHEET_ID = '1dQJqfypqLYssxCj--e3rt5Ufv-8olgJp1mwA_erjX_0'
RANGE_NAME = 'Sheet1'

def load_google_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    if not values:
        raise Exception('No data found in Google Sheet.')
    headers = values[0]
    rows = values[1:]
    data = []
    for row in rows:
        obj = {}
        for i, header in enumerate(headers):
            obj[header] = row[i] if i < len(row) else None
        data.append(obj)
    return data

def load_drive():
    credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    service = build('drive', 'v3', credentials=credentials)
    return service

class IdGenerator:
    def __init__(self):
        self.current_id = 1
    def get_next(self):
        id_ = self.current_id
        self.current_id += 1
        return id_

def build_family_tree(data):
    drive_srv = load_drive()
    
    id_gen = IdGenerator()
    name_to_data = OrderedDict()
    for d in data:
        name_to_data[d['name']] = d
    unique_data = list(reversed(list(name_to_data.values())))

    name_to_id = {}
    parent_nodes = {}
    litters = {}
    parents_partners = defaultdict(set)
    has_descendants = set()
    child_to_parents = {}  # ИСПРАВЛЕНИЕ: сохраняем связи fid/mid для каждого ребёнка

    def get_or_create_id(name, gender=None):
        if not name:
            return None
        if name not in name_to_id:
            new_id = id_gen.get_next()
            name_to_id[name] = new_id
            parent_nodes[new_id] = {
                'id': new_id, 'name': name, 'gender': gender,
                'isParent': True, 'pids': set()
            }
        return name_to_id[name]

    # Присвоить id всем собакам
    for d in unique_data:
        name_to_id.setdefault(d['name'], id_gen.get_next())

    # Обработать родителей, партнёрства, потомков и связи детей
    for d in unique_data:
        fid = get_or_create_id(d.get('father'), 'male')
        mid = get_or_create_id(d.get('mother'), 'female')
        d['fid'] = fid
        d['mid'] = mid
        child_id = name_to_id[d['name']]
        child_to_parents[child_id] = (fid, mid)  # ИСПРАВЛЕНИЕ
        if fid: has_descendants.add(fid)
        if mid: has_descendants.add(mid)
        if fid and mid:
            parents_partners[fid].add(mid)
            parents_partners[mid].add(fid)

    # Создать помёты
    for d in unique_data:
        fid = d['fid']
        mid = d['mid']
        birthdate = d.get('birthdate')
        if fid is None or mid is None or not birthdate: continue
        litter_key = f'{fid}_{mid}_{birthdate}'
        if litter_key not in litters:
            litters[litter_key] = {'id': id_gen.get_next(), 'puppies': []}
        litters[litter_key]['puppies'].append(d)
        d['stpid'] = litters[litter_key]['id']
        d['litter_key'] = litter_key

    # Фильтрация помётов: только без потомков у щенков
    filtered_litters = {}
    for litter_key, litter_data in litters.items():
        puppies = litter_data['puppies']
        if all(name_to_id[pup['name']] not in has_descendants for pup in puppies):
            filtered_litters[litter_key] = litter_data
        else:
            for pup in puppies:
                pup.pop('stpid', None)

    nodes = []

    # Родители с партнёрами
    for pid, node in parent_nodes.items():
        node['pids'] = list(parents_partners[pid]) if parents_partners[pid] else None
        node_data = {'id': node['id'], 'name': node['name'], 'gender': node['gender']}
        if node['pids']: node_data['pids'] = node['pids']
        node_data['isParent'] = True
        nodes.append(node_data)

    # Помёты из filtered_litters
    for litter_key, litter_data in filtered_litters.items():
        puppies = litter_data['puppies']
        litter_char = None
        for pup in puppies:
            pass_name = pup.get('pass_name')
            if pass_name:
                words = re.split(r'\s+', pass_name.strip())
                if len(words) >= 2:
                    litter_char = words[1][0]
                    break
        if not litter_char: litter_char = '?'
        for pup in puppies:
            pass_name = pup.get('pass_name')
            if pass_name:
                words = re.split(r'\s+', pass_name.strip())
                if len(words) < 2 or words[1][0] != litter_char:
                    litter_char = '!'
                    break
        fid, mid, _ = litter_key.split('_')
        litter_node = {
            'id': litter_data['id'], 'name': f'Помёт {litter_char}',
            'fid': int(fid), 'mid': int(mid), 'gender': None,
            'isLitter': True, 'tags': ['node-with-subtrees']
        }
        nodes.append(litter_node)

    # Щенки с восстановленными fid/mid и stpid
    for d in unique_data:
        child_id = name_to_id[d['name']]
        fid, mid = child_to_parents.get(child_id, (None, None))  # ИСПРАВЛЕНИЕ: связи сохранены

        gender = d.get('gender')
        if gender:
            gender = gender.strip().upper()
            gender = 'male' if gender == 'М' else 'female' if gender == 'Ж' else None

        node = {
            'id': child_id, 'name': d['name'], 'gender': gender,
            'birthdate': datetime.strptime(d.get('birthdate'), "%m/%d/%Y").strftime("%Y-%m-%d"),
            'pass_name': d.get('pass_name'), 
        }
        if d.get('litter_key') in filtered_litters:
            node['stpid'] = d.get('stpid')
            node['fid'] = None 
            node['mid'] = None
        else:
            node['fid'] = fid
            node['mid'] = mid
        
        if d.get('web') is not None:
            node['web'] = d.get('web').replace(' ', '\n')
        for k, v in d.items():
            if k not in {'name', 'gender', 'birthdate', 'pass_name', 'stpid', 'fid', 'mid', 'father', 'mother', 'timestamp', 'url_photo', 'isParent', 'web'}:
                if v is not None:
                    node[k] = v
        nodes.append(node)

        # Фото (без изменений)
        url_photo = d.get('url_photo')
        if url_photo:
            photo_id = extract_drive_file_id(url_photo)
            if photo_id:
                dog_name_translit = 'dog_photos/' + rus_to_translit(node['name']) + "_" + d.get('timestamp').replace('/', '_').replace(' ', '_').replace(":", '_') + '.jpg'
                if not os.path.exists(dog_name_translit):
                    request = drive_srv.files().get_media(fileId=photo_id)
                    fh = io.FileIO(dog_name_translit, 'wb')
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        print(f"Загрузка {dog_name_translit} {int(status.progress() * 100)}%")
                    fh.close()
                else:
                    print(f'Skip {dog_name_translit}')
                node['photo'] = dog_name_translit

    return nodes

def main():
    data = load_google_sheet()
    tree_nodes = build_family_tree(data)
    with open('family_tree.json', 'w', encoding='utf-8') as f:
        json.dump(tree_nodes, f, ensure_ascii=False, indent=2)
    print('Сохранено в family_tree.json')

if __name__ == '__main__':
    main()
