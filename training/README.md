Dataset generation and Training process



This project colects historical Waze alerts, jams data and Vida Segura accidents data and merge them in a single, coherent dataset.
After the dataset is ready, we can proceed to train a machine learning algorithm for the probability prediction of a accident having victims.
The merge result is saved to a csv file, and the prediction model is saved to disk as well. The model will be used in the real time backend application.


Dependencies

Python=3.8

requests=2.25.1
pandas=1.2.4
geopandas=0.9.0
shapely=1.7.1
pyproj=3.1.0
imblearn=0.8.0
catboost=0.26
boto3=1.17.97
retrying=1.3.3
dateutil=2.8.1
dask=2021.06.0



With the python environment and all dependencies installed, we need to configure the aws environment to search and download waze historical data. We need to create two files:

Credential file has the following format:

[default]
aws_access_key_id=
aws_secret_access_key=

The values of each field is provided by FGV. The file needs to be saved in the following path:

~/.aws/credentials (on Linux)
%UserProfile%/.aws/credentials (on Windows)



Config file has the following format:

[default]
region=

The values of each field is provided by FGV. The file needs to be saved in the following path:

~/.aws/config (on Linux)
%UserProfile%/.aws/config (on Windows)

With the python environment and all dependencies installed and aws configuration conplete, run in the command line:

>python download-process-generate.py &

ATTENTION:
This execution can take up to many days of non stop processing.
The result of this processing is saved in the data/dataset folder.

Last, we execute the following:

>python train_model.py &

After process is done, it will save the model in the data folder.