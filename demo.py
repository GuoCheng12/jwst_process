from astroquery.mast import Observations
from astropy.table import Table, vstack
import pandas as pd
import pdb
from tqdm import tqdm
import requests
import yaml
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from astroquery.mast import Observations
from astropy.table import Table, vstack
import pandas as pd
import os
import yaml
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 加载配置文件
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# 获取配置参数
filters_list = config['filters_list']
base_dir = config['all']['workspace_dir']
download_cfg = config['jwst']['download_cfg']
catalog_cfg = config['jwst']['catalog_cfg']
metainfo_tag = os.path.join(base_dir, download_cfg['metainfo_tag'])
download_tag = os.path.join(base_dir, download_cfg['download_tag'])
os.makedirs(metainfo_tag, exist_ok=True)
os.makedirs(download_tag, exist_ok=True)

# 查询并下载数据
all_results = []
for filt in filters_list:
    print(f"Querying filter: {filt}")
    obs_table = Observations.query_criteria(
        calib_level=catalog_cfg['calib_level'],
        dataproduct_type=catalog_cfg['dataproduct_type'],
        intentType=catalog_cfg['intentType'],
        obs_collection=catalog_cfg['obs_collection'],
        filters=[filt]
    )

    if len(obs_table) == 0:
        print(f"No observations found for filter: {filt}")
        continue

    obs_table_df = Table(obs_table).to_pandas()

    # 按观测 ID 逐个获取 product list
    product_list = []
    for obs in tqdm(obs_table, desc=f"Retrieving product list for filter {filt}"):
        single_product = Observations.get_product_list(obs['obsid'])
        product_list.append(single_product)
        break
    products = vstack(product_list)
    products_df = Table(products).to_pandas()

    if 'dataURI' not in products_df.columns:
        print(f"'dataURI' column not found, skipping filter: {filt}")
        continue

    fits_files_df = products_df[(products_df['productFilename'].str.endswith('.fits')) & products_df['dataURI'].notna()]
    pdb.set_trace()
    obs_df = obs_table_df[['obsid', 't_exptime', 'filters', 's_ra', 's_dec', 'instrument_name']]
    obs_df.columns = ['obsid', 'Exposure Time', 'Filters', 'RA', 'Dec', 'Instrument']

    merged_df = pd.merge(obs_df, fits_files_df[['obsID', 'productFilename', 'dataURI']], 
                         left_on='obsid', right_on='obsid', how='left').dropna(subset=['dataURI'])
    all_results.append(merged_df)

final_df = pd.concat(all_results, ignore_index=True)

# 保存 metadata 信息至 JSON
def save_metadata_to_json(observation_id, group):
    meta_infos = []
    for _, row in group.iterrows():
        meta_info = {
            "file": f"{observation_id}/{row['productFilename']}",
            "exptime": str(row['Exposure Time']),
            "filter": row['Filters'],
            "ra": str(row['RA']),
            "dec": str(row['Dec']),
            "meta": {
                "calibration_level": catalog_cfg['calib_level'],
                "instrument_name": row['Instrument'],
            }
        }
        meta_infos.append(meta_info)
    json_path = os.path.join(metainfo_tag, f"{observation_id}.json")
    with open(json_path, 'w') as f:
        json.dump(meta_infos, f, indent=4)
    print(f"Saved metadata JSON for {observation_id} at {json_path}")

for observation_id, group in final_df.groupby("Observation ID"):
    save_metadata_to_json(observation_id, group)

# 下载文件函数
def download_products_for_group(group, observation_id):
    # 确保该组的文件夹存在
    group_dir = os.path.join(download_tag, observation_id)
    os.makedirs(group_dir, exist_ok=True)
    
    # 下载符合条件的 .fits 文件
    obsids = group['obsid'].dropna().unique().tolist()
    if obsids:
        try:
            pdb.set_trace()
            Observations.download_products(obsids, download_dir=group_dir)
            print(f"Downloaded products for group {observation_id}")
        except Exception as e:
            print(f"Error downloading group {observation_id}: {e}")
    else:
        print(f"No valid obsids found for group: {observation_id}")

# 多线程下载每组 .fits 文件
with ThreadPoolExecutor(max_workers=download_cfg['max_threads']) as executor:
    futures = {
        executor.submit(download_products_for_group, group, observation_id): observation_id
        for observation_id, group in final_df.groupby("Observation ID")
    }
    for future in as_completed(futures):
        observation_id = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Failed to download for group {observation_id}: {e}")

print("All downloads complete!")
