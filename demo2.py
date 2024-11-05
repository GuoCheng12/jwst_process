import pandas as pd
from astroquery.mast import Observations
from concurrent.futures import ThreadPoolExecutor, as_completed
import pdb

from astropy.io import fits
import matplotlib.pyplot as plt

# 指定 .fits 文件路径
file_path = "/Users/wuguocheng/workshop/jwst/output/jw01063-o184_t028_niriss_clear-f090w_i2d.fits"

# 打开 .fits 文件
with fits.open(file_path,memmap=True) as hdul:
    # 查看 .fits 文件的信息
    hdul.info()
    
    # 假设图像数据在第一个扩展中（通常为 [1]，具体位置请根据 .info() 输出确认）
    image_data = hdul[1].data  # 如果在 PRIMARY，则使用 hdul[0].data
    #image_data = hdul[1].data[:100, :100]  # 只读取前100行和100列

# 检查数据是否成功加载
if image_data is not None:
    # 绘制图像
    plt.figure(figsize=(10, 10))
    plt.imshow(image_data, cmap='gray', origin='lower')
    plt.colorbar()
    plt.title("FITS Image")
    plt.show()
else:
    print("Error: No image data found in this FITS file.")