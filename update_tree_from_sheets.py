import json
import re
from collections import defaultdict, OrderedDict
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime


# Конфигурация: замените на ваши путь к credentials json и ID таблицы
SERVICE_ACCOUNT_FILE = 'samolla-b4398f9d675c.json'
SPREADSHEET_ID = '1dQJqfypqLYssxCj--e3rt5Ufv-8olgJp1mwA_erjX_0'
RANGE_NAME = 'Sheet1'  # или ваш лист


def load_google_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                               range=RANGE_NAME).execute()
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

class IdGenerator:
    def __init__(self):
        self.current_id = 1
    def get_next(self):
        id_ = self.current_id
        self.current_id += 1
        return id_

def build_family_tree(data):
    id_gen = IdGenerator()
    name_to_data = OrderedDict()

    # Удалить дубликаты по имени, оставляя последнюю встречу
    for d in reversed(data):
        name_to_data[d['name']] = d
    unique_data = list(reversed(list(name_to_data.values())))

    name_to_id = {}
    parent_nodes = {}
    litters = {}  # litter_key -> {'id': int, 'puppies': list}
    parents_partners = defaultdict(set)

    def get_or_create_id(name, gender = None):
        if not name:
            return None
        if name not in name_to_id:
            new_id = id_gen.get_next()
            name_to_id[name] = new_id
            parent_nodes[new_id] = {
                'id': new_id,
                'name': name,
                'gender': gender,
                'isParent': True,
                'pids': set()
            }
        return name_to_id[name]

    # Присвоить id всем собакам
    for d in unique_data:
        if d['name'] not in name_to_id:
            name_to_id[d['name']] = id_gen.get_next()

    # Получить id родителей и записать партнёрства
    for d in unique_data:
        fid = get_or_create_id(d.get('father'), 'male')
        mid = get_or_create_id(d.get('mother'), 'female')
        d['fid'] = fid
        d['mid'] = mid
        if fid and mid:
            parents_partners[fid].add(mid)
            parents_partners[mid].add(fid)

    # Создать помёты (литтеры)
    for d in unique_data:
        fid = d['fid']
        mid = d['mid']
        birthdate = d.get('birthdate')
        if fid is None or mid is None or not birthdate:
            continue
        litter_key = f'{fid}_{mid}_{birthdate}'
        if litter_key not in litters:
            litters[litter_key] = {'id': id_gen.get_next(), 'puppies': []}
        litters[litter_key]['puppies'].append(d)
        d['stpid'] = litters[litter_key]['id']

    nodes = []

    # Добавить родителей с партнёрами
    for pid, node in parent_nodes.items():
        node['pids'] = list(parents_partners[pid]) if parents_partners[pid] else None
        node_data = {
            'id': node['id'],
            'name': node['name'],
            'gender': node['gender']
        }
        if node['pids']:
            node_data['pids'] = node['pids']
        node_data['isParent'] = True
        nodes.append(node_data)

    # Добавить помёты с именем из pass_name и тегом
    for litter_key, litter_data in litters.items():
        puppies = litter_data['puppies']
        litter_char = None
        # Определяем первую букву второго слова pass_name любого щенка из помёта
        for pup in puppies:
            pass_name = pup.get('pass_name')
            if pass_name:
                words = re.split(r'\s+', pass_name.strip())
                if len(words) >= 2:
                    litter_char = words[1][0]
                    break
        if not litter_char:
            litter_char = '?'
        # Проверяем совпадение буквы вторых слов у всех щенков
        for pup in puppies:
            pass_name = pup.get('pass_name')
            if pass_name:
                words = re.split(r'\s+', pass_name.strip())
                if len(words) < 2 or words[1][0] != litter_char:
                    litter_char = '!'
                    break

        fid, mid, birthdate = litter_key.split('_')
        litter_node = {
            'id': litter_data['id'],
            'name': f'Помёт {litter_char}',
            'fid': int(fid),
            'mid': int(mid),
            'gender': None,
            'isLitter': True,
            'tags': ['node-with-subtrees']
        }
        nodes.append(litter_node)

    # Добавить щенков, конвертируя gender, исключая игнорируемые поля
    IGNORE_FIELDS = {'timestamp', 'url_photo'}

    for d in unique_data:
        gender = d.get('gender')
        if gender:
            gender = gender.strip().upper()
            if gender == 'М':
                gender = 'male'
            elif gender == 'Ж':
                gender = 'female'
            else:
                gender = None

        node = {
            'id': name_to_id[d['name']],
            'name': d['name'],
            'gender': gender,
            'birthdate': datetime.strptime(d.get('birthdate'), "%m/%d/%Y").strftime("%Y-%m-%d"),
            'pass_name': d.get('pass_name'),
            'stpid': d.get('stpid'),
            'fid': None,
            'mid': None,
        }
        # Добавляем все дополнительные поля, кроме служебных и игнорируемых
        for k, v in d.items():
            if k not in {'name', 'gender', 'birthdate', 'pass_name', 'stpid', 'fid', 'mid', 'Timestamp', 'url_photo'} and k not in IGNORE_FIELDS:
                if v is not None:
                    node[k] = v
        nodes.append(node)

    return nodes

def main():
    data = load_google_sheet()
    tree_nodes = build_family_tree(data)
    with open('family_tree.json', 'w', encoding='utf-8') as f:
        json.dump(tree_nodes, f, ensure_ascii=False, indent=2)
    print('Сохранено в family_tree.json')

if __name__ == '__main__':
    main()
