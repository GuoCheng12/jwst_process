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
# 加载配置文件

def open_proxy():
    os.environ['http_proxy'] = 'http://wuguocheng:t10buLavYLGlT7PfOjA5RaZm32ASjYakpQ2q6GFKcblC5t2jFpgcKx9v6Xtt@10.1.20.50:23128'
    os.environ['https_proxy'] = 'http://wuguocheng:t10buLavYLGlT7PfOjA5RaZm32ASjYakpQ2q6GFKcblC5t2jFpgcKx9v6Xtt@10.1.20.50:23128'
    os.environ['HTTP_PROXY'] = 'http://wuguocheng:t10buLavYLGlT7PfOjA5RaZm32ASjYakpQ2q6GFKcblC5t2jFpgcKx9v6Xtt@10.1.20.50:23128'
    os.environ['HTTPS_PROXY'] = 'http://wuguocheng:t10buLavYLGlT7PfOjA5RaZm32ASjYakpQ2q6GFKcblC5t2jFpgcKx9v6Xtt@10.1.20.50:23128'

def close_proxy():
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)

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
    obs_df = obs_table_df[['obs_id', 't_exptime', 'filters', 's_ra', 's_dec', 'instrument_name']]
    obs_df.columns = ['Observation ID', 'Exposure Time', 'Filters', 'RA', 'Dec', 'Instrument']

    merged_df = pd.merge(obs_df, fits_files_df[['obs_id', 'productFilename', 'dataURI']], 
                         left_on='Observation ID', right_on='obs_id', how='left').dropna(subset=['dataURI'])
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
            "dataURI": row['dataURI'],  # 添加 dataURI 字段
            "meta": {
                "calibration_level": catalog_cfg['calib_level'],
                "instrument_name": row['Instrument'],
            }
        }
        meta_infos.append(meta_info)
    json_path = os.path.join(metainfo_tag, f"{observation_id}.json")
    with open(json_path, 'w') as f:
        json.dump(meta_infos, f, indent=4)

    print(f"Saved metadata JSON with dataURI for {observation_id} at {json_path}")

# 根据 Observation ID 分组，保存每个组的 JSON 文件
for observation_id, group in final_df.groupby("Observation ID"):
    save_metadata_to_json(observation_id, group)

print("All JSON files with dataURI saved.")

# 下载文件函数
def download_and_upload_to_ceph(data_uri, filename, download_directory, observation_id):
    url = f"https://mast.stsci.edu/api/v0.1/Download/file?uri={data_uri}"
    filepath = os.path.join(download_directory, filename)

    try:
        open_proxy()
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            os.makedirs(download_directory, exist_ok=True)
            total_size = int(response.headers.get('content-length', 0))
            with open(filepath, 'wb') as f, tqdm(
                total=total_size, unit='B', unit_scale=True, desc=filename
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
            print(f"Download complete: {filename}")

            close_proxy()
            ceph_path = f"s3://JWST/{observation_id}/"
            upload_command = f'aws s3 cp {filepath} {ceph_path} --endpoint-url=http://10.140.31.252:80'
            os.system(upload_command)
            print(f"Upload complete: {filename} to {ceph_path}")
            os.remove(filepath)
            print(f"Deleted local file: {filename}")
            open_proxy()

        else:
            close_proxy()
            print(f"Download failed: {filename}, status code: {response.status_code}")

    except Exception as e:
        print(f"Error downloading {filename}: {e}")


start_id = 'jw01187-c1007_t003_nircam_clear-f070w'
start_download = False


for observation_id, group in final_df.groupby("Observation ID"):
    # 检查是否达到了开始下载的 ID
    if not start_download:
        if observation_id == start_id:
            start_download = True  # 达到目标 ID，开始下载
        else:
            continue  # 跳过当前 ID

    group_dir = os.path.join(download_tag, observation_id)
    with ThreadPoolExecutor(max_workers=download_cfg['max_threads']) as executor:
        futures = {
            executor.submit(download_and_upload_to_ceph, row['dataURI'], row['productFilename'], group_dir, observation_id): row['productFilename']
            for _, row in group.iterrows()
        }
        for future in as_completed(futures):
            filename = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
    print(f"Completed downloads and uploads for group: {observation_id}")

print("All downloads complete!")





