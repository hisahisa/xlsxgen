# xlsxgen
  - pip install で xlsxgenのモジュールを落として、pythonコードを以下の通り記述する。
    - スピード重視の場合はメソッド(process_bytes)をコメント解除する
    - メモリ容量を抑えたい場合はメソッド(process_bytes_zlib)と、zlipの圧縮処理の箇所をコメント解除する

# 使い方

```python
import pandas as pd
import zipfile
import xlsxgen
import resource
import time
import io
import zlib

# 開始
start_time = time.perf_counter()

file_name = 'tenpo_shohin_pattern3.xlsx'
excel_sheet = 'sheet1'

with open(file_name, 'rb') as file_obj:
    with zipfile.ZipFile(file_obj, 'r') as zip_ref:
        # エクセルファイルを展開してオブジェクトを取得
        s_list = [f'xl/worksheets/{excel_sheet}.xml']
        sheet_list = [i for i in zip_ref.namelist() if i in s_list]
        solve_bytes = 'xl/sharedStrings.xml'
        style_bytes = 'xl/styles.xml'
        bytes_obj = zip_ref.read([i for i in zip_ref.namelist() if i in sheet_list][0])
        bytes_solve_obj = zip_ref.read(solve_bytes)
        bytes_style_obj = zip_ref.read(style_bytes)

        try:
            generator = xlsxgen.DataGenerator()

            # スピード重視のケース(メモリ容量は大きく消費する)
            generator.process_bytes(10000, bytes_obj, bytes_solve_obj)

            # メモリ容量を抑えたいケース(スピードは少し遅い)
            # bytes_obj = zlib.compress(bytes_obj)
            # bytes_solve_obj = zlib.compress(bytes_solve_obj)
            # generator.process_bytes_zlib(10000, bytes_obj, bytes_solve_obj)
            del bytes_obj, bytes_solve_obj
            while True:
                csv_data = generator.generate_data_chunk()
                if csv_data == "finish":
                    print(csv_data)
                    break
                if csv_data:
                    print(csv_data)
                    #df = pd.read_csv(io.StringIO(csv_data), header=None)
                    #print(df)
        except ValueError as e:
            print(f"value error: {e}")

memory_info = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"Max RSS: {memory_info / 1024 / 1024} MB")

end_time = time.perf_counter()
# 経過時間を出力(秒)
elapsed_time = end_time - start_time
print(elapsed_time)

```