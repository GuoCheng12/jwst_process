import pandas as pd
import pdb
import os

save_dir  = 'jwst_filter'
data = pd.read_excel('/Users/wuguocheng/workshop/jwst/JWST_fliters.xlsx', sheet_name=None)
# 定义保存数据的目录


for structure_name in data.keys():
    for filter_id in data[structure_name]['Filter ID']:
        download_url = f"http://svo2.cab.inta-csic.es/svo/theory/fps3/getdata.php?format=ascii&id={filter_id}"
        
        filter_dir = os.path.join(save_dir, structure_name)
        os.makedirs(filter_dir, exist_ok=True)
        save_path = os.path.join(filter_dir, f"{filter_id.replace('/', '_')}.data")
        
        cmd = f'wget "{download_url}" -O "{save_path}"'
        print(f"Downloading {filter_id} to {save_path}")
        os.system(cmd)
# http://svo2.cab.inta-csic.es/svo/theory/fps3/getdata.php?format=ascii&id=JWST/NIRCam.F070W
# http://svo2.cab.inta-csic.es/svo/theory/fps3/getdata.php?format=ascii&id=JWST/NIRCam.F090W