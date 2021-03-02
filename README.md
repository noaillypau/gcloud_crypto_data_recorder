# gcloud_crypto_data_recorder

Python asyncronous scripts to recoord crypto exchanges data using ccxt library. \
You can edit the batches within the jupyter notebook. \
Then create a vm and launch parallel scripts with tmux for each exchange. \
You should also launch the uploader that will take the csv files and upload them to the gcloud bucket daily.
