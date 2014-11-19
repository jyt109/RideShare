#RideMeter 
###Measuring the value of shared rides

##Project summary 
This project takes [New York Taxi ride data from 2013](http://chriswhong.com/open-data/foil_nyc_taxi/) and calculate the amount of money/ miles that could be saved if rides are shared. A score is calculated to indicate how shareable each ride is. A webapp is also written in Flask for visualization of the shared path for each of hypothetical shared ride.  

##Stage 0 

####Install OSRM routing API
This project uses OSRM to get the route between 2 points and this is achieved by OSRM.

Instructions are as below.

1. Install dependencies:
https://github.com/Project-OSRM/osrm-backend/wiki/Building%20OSRM#mac-os-x-1071-1082
2. Fetch and compile source: 
https://github.com/Project-OSRM/osrm-backend/wiki/Building%20OSRM#fetch-the-source
3. Download OSM data:
http://download.geofabrik.de/north-america/us/new-york.html
4. Running the OSRM Engine
https://github.com/Project-OSRM/osrm-backend/wiki/Running-OSRM#extracting-the-road-network
5. API Usage (Used in project in query_osrm.py)
https://github.com/Project-OSRM/osrm-backend/wiki/Server-api


##Stage 1

####preprocessing.py, eda.py & db_interface.py
preproessing.py contains the class **Preprocess** which cleans the data and push it into PostGres (POSTGIS) database.
This is followed by some EDA(exploratory data analysis) to get a sense of how the data is like.
db_interface.py contains the class **DBRideShare** which handles all the interaction with the database.
preprocessing.py, query_osrm.py, match_rideshare.py and scoring_rideshare.py all inherit from the **DBRideShare** class

##Stage 2
####query_osrm.py
Contains the class **QueryOSRM** which queries the OSRM routing API engine to get the routes between the pickup and dropoff locations of the taxi rides. **QueryOSRM** also merge pre-download data of [bounds of New York districts](http://download.geofabrik.de/north-america/us/new-york.html) and decide what districts the pickup and dropoff points are. The route and district data is subsequently pushed into PostGres for later steps.

##Stage 3
####match_rideshare.py
Contains the class MatchRideShare that does the matching of rides that could be shared and then filter by various conditions to trim down the number of matched rides to good quality ones for querying and other more expensive computation downstream.
 
##Stage 4
####scoring_rideshare.py
Contains the class ScoringRideShare that scores the filtered rides from Stage 3 and pick the best ride that has the top score. This results in 7,847 / 24,864 rides being matched. This module needs cleaning up. Most of the code is still in an IPython notebook. 

##Final Stage
####webappy
The folder webappy contain the whole Flask webapp that does the visualization and loading of the 7,847 shared ride entries into a table for browsing.

The file structure of webappy follows the standard Flask structure:
- **templates** contains the Jinja HTML files
- **app.py** handles the server-side processing
- **static** contains all the JS and CSS libraries
- **data** contains the results from **Stage 4** formatted in a way that can be read into the web app




