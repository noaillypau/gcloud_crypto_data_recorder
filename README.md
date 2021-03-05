# gcloud_crypto_data_recorder

Python asyncronous scripts to recoord crypto exchanges data using ccxt library. \
You can edit the batches within the jupyter notebook. \
Then create a vm and launch parallel scripts with tmux for each exchange. \
You should also launch the uploader that will take the csv files and upload them to the gcloud bucket daily.

# Deploy on Google VM

## Init VM

```ssh
sudo apt-get install python3-pip
sudo apt-get install tmux
sudo apt-get install git

git clone https://github.com/noaillypau/gcloud_crypto_data_recorder
pip3 install -r requirements.txt
```

## Run updater

```ssh
tmux

tmux rename-session -t 0 uploader
cd gcloud_crypto_data_recorder
python3 uploader/main_uploader.py
```

## Run updater

```ssh
tmux

tmux rename-session -t ID_TMUX EXCHANGE
cd gcloud_crypto_data_recorder
python3 recorder/main_record_exchange.py EXCHANGE BATCH
```

# Debuging
```ssh
python3 recorder/main_record_exchange.py EXCHANGE BATCH -d
```
or

```ssh
python3 recorder/main_record_exchange.py EXCHANGE BATCH --debug
```
