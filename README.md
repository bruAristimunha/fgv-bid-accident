Real time Waze Accidents Probabilities



This project colects real-time Waze alerts and jams data and estimates a probability of an accident having victims. Before executing, we need to add the file "polygon.json", given by FGV, to the data folder. This file has sensitive information which can't be shared publicly.
The result is saved to a csv, which will be used in the front end application.
This csv is updated every minute, registering the accident alerts occured in the last ten minutes.

Dependencies

Python=3.8

requests=2.25.1
pandas=1.2.4
geopandas=0.9.0
shapely=1.7.1
pyproj=3.1.0
catboost=0.26

Before executing, we need to add the file "polygon.json", given by FGV, to the data folder. This file has sensitive information which can't be shared publicly.

With the python environment and all dependencies installed, run in the command line:

>python waze-real-time.py &


To initialize the front-end just:

>python front-end/dash_app.py 

Or real-time:

>python front-end/dash_app-real-time.py 

