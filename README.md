# Rapy_Genius

Rapy is a package that uses the Genius API to collect data from genius.com as songs, artists and lyrics 

This framework has two modules:
    genius_api: Contains the GeniusApiManager class.
    data_managger: Contains the GeniusApiDatabaseManager class that uses MongoDB and pymongo to store data and use it

##### Visit https://docs.genius.com/ to get API client access.

## Installation

Run the following to install

'''python

    pip install rapy_genius

'''

## Usage

'''python
    from genius_api import GeniusApiManager
    from data_manager import GeniusApiDatabaseManager
    API_CLIENT_ACCESS_TOKEN = "XXXXXXXXX" #The Api client access token generated from https://docs.genius.com/
    api_manager = GeniusApiManager(API_CLIENT_ACCESS_TOKEN)
    db_manager = GeniusApiDatabaseManager(db_name='Genius_scrapper')
    add_artist(45) #Eminem
'''


# Developing Rapy Genius

to install rapy_genius along with the tools you need to develop and run tests

'''bash

    pip install -e .[dev]

'''





