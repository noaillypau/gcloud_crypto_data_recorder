import datetime, time, os, json
# numpy
try:
    import numpy as np
except:
    os.system('pip install numpy')
    import numpy as np
# google storage
try:
    from google.cloud import storage
except:
    os.system('pip install google-cloud-storage')
    from google.cloud import storage

SEPARATOR = '_'
TIME_TO_SLEEP = 5#60*60*4

def date_to_datestr(date):
    return date.strftime('%Y%m%d')

def filename_to_datestr(filename):
    return filename[-12:-4]

def filename_to_bucket(source_file_name):    
    # open config
    with open("configs/config.json",'r') as f:
        config = json.load(f)
        f.close()
    # set target of gcloud creds
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'configs/gcloud_client.json'
    # exctract output path 
    splited_filename = source_file_name.split(SEPARATOR)
    exchange, symbol, datestr = splited_filename[0], splited_filename[1], splited_filename[2]
    dest_path = config['path_bucket'].replace('exchange',exchange).replace('symbol',symbol).replace('datestr',datestr)
    # exctract bucket name
    bucket_name = config['name_bucket']
    # move file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(source_file_name)  

def upload_files():
    current_day = datetime.datetime.now()
    last_day = current_day-datetime.timedelta(days=1)
    first_day = current_day - 30*datetime.timedelta(days=1)
    list_date_str = []
    # create list of allowed datestr
    while first_day < last_day:
        list_date_str.append(date_to_datestr(first_day)) 
        first_day += datetime.timedelta(days=1)    
    # loop on file in data folder
    for filename in os.listdir('data'):
        if filename[-4:] == '.csv' and filename_to_datestr(filename) in list_date_str:
            print('  uploading file ',filename)
            filename_to_bucket('data/'+filename)
            time.sleep(0.5)
            os.remove('data/'+filename)

def loop():
    current_day = datetime.datetime.now() - datetime.timedelta(days=2)
    while True:
        if datetime.datetime.now().day != current_day.day:
            print('\nNew day ',datetime.datetime.now())
            upload_files()
            current_day = current_day + datetime.timedelta(days=1)
            print('')
        else:
            time.sleep(TIME_TO_SLEEP)
        break

loop()


