import requests
import io
import zipfile
import shutil

STOREPATH = '/data/csv/'

def download_extract_zip(url, dirpath):
   response = requests.get(url)
   with zipfile.ZipFile(io.BytesIO(response.content)) as zfile:
      store_at = STOREPATH + dirpath
      zfile.extractall( store_at )

def download_chunk(url, dirpath):
    path = dirpath + 'dataset.zip'
    r = requests.get(url, stream = True)
    with open(path, 'wb') as f:
       for ch in r:
          f.write(ch)

def unzip_chunk( dirpath ):
    path = dirpath + 'dataset.zip'
    with open(path, 'rb') as zf:
       with zipfile.ZipFile( zf, allowZip64=True ) as zfile:
          for member in zfile.infolist():
             store_at = dirpath + member.filename
             with open(store_at, 'wb') as outfile, zfile.open(member) as infile:
                shutil.copyfileobj(infile, outfile)

url = 'http://api.gbif.org/v1/occurrence/download/request/0020324-191105090559680.zip'
dirpath = '/data/csv/'

download_chunk(url, dirpath)
unzip_chunk( dirpath )
