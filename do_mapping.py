import json
import re
from difflib import get_close_matches

ALIASES = {
    'sku': ['seller sku', 'item sku', '商品sku', 'sku编号'],
    'product name': ['item name', 'title', '商品名称', '标题', '名称'],
    'brand': ['品牌', 'brand name'],
    'price': ['售价', '价格', 'list price', 'sale price', '价格(usd)'],
    'upc': ['gtin', 'barcode', '条码'],
    'description': ['产品描述', '描述', 'product description'],
    'color': ['颜色', 'colour'],
    'size': ['尺码', '尺寸'],
}

def normalize_header(value):
    if value is None:
        return ''
    text = str(value).strip().lower()
    text = re.sub(r'[\s\-_/()\[\]{}:：]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def build_alias_lookup():
    alias_lookup = {}
    for canonical, aliases in ALIASES.items():
        alias_lookup[normalize_header(canonical)] = normalize_header(canonical)
        for alias in aliases:
            alias_lookup[normalize_header(alias)] = normalize_header(canonical)
    return alias_lookup

def canonicalize(header, alias_lookup):
    normalized = normalize_header(header)
    return alias_lookup.get(normalized, normalized)

template_headers = {
    1: '图片',
    2: 'ASIN', 
    3: '标题',
    4: '产品卖点',
    5: '详细参数',
    6: 'price',
    7: 'SKU'
}

source_headers = {
    1: '标题',
    2: '产品卖点',
    3: '详细参数',
    4: 'price',
    5: 'SKU'
}

alias_lookup = build_alias_lookup()

source_index = {}
for col, name in source_headers.items():
    source_index[canonicalize(name, alias_lookup)] = col

mappings_list = []
unmapped = []

source_keys = list(source_index.keys())

for tpl_col, tpl_name in template_headers.items():
    target_key = canonicalize(tpl_name, alias_lookup)
    
    if target_key in source_index:
        mappings_list.append({
            'template_header': tpl_name,
            'source_header': source_headers[source_index[target_key]],
            'template_col': tpl_col,
            'source_col': source_index[target_key]
        })
        continue
    
    guess = get_close_matches(target_key, source_keys, n=1, cutoff=0.82)
    if guess:
        mappings_list.append({
            'template_header': tpl_name,
            'source_header': source_headers[source_index[guess[0]]],
            'template_col': tpl_col,
            'source_col': source_index[guess[0]]
        })
    else:
        unmapped.append(tpl_name)

result = {
    'mappings': mappings_list,
    'unmapped_template_headers': unmapped
}

output_file = open('mapping_result.json', 'w', encoding='utf-8')
json.dump(result, output_file, ensure_ascii=False, indent=2)
output_file.close()

print(json.dumps(result, ensure_ascii=False, indent=2))
