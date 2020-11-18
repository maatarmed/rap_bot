import requests
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import re
import os

"""
# genius API base and client_token
API_BASE = "https://api.genius.com"
API_CLIENT_ACCESS_TOKEN = "w7XemCkBJW-a0eT5RZ3bjHKt9-fr-v0RYBYS4iQ_9CtY-uvbJomzCpuqoonzw6jM"
"""

class GeniusApiManager:
    def __init__(self, base, access_token):
        self.base = base
        self.access_token = access_token

    def __get_json(self, path, params=None, headers=None):
        """
        Generate Request URL and Get response object from querying the genius API

        Args:
            path (string): url to get data from
            params (optional): request parameters. Defaults to None.
            headers (optional): check if Authorization existes create one otherwise. Defaults to None.

        Returns:
            json : request result
        """
        # Generate request URL
        requrl = '/'.join([self.base, path])
        token = "Bearer {}".format(self.access_token)
        if headers:
            headers['Authorization'] = token
        else:
            headers = {"Authorization": token}
        # Get response object from querying genius api
        response = requests.get(url=requrl, params=params, headers=headers)
        response.raise_for_status()
        return response.json()


    def search(self, query, typee='artist'):
        """
        This is a one function to get data through the Genius API

        Args:
            typee (string) : takes one of the following values : artist - song
            query (string / int) : string when type search, int otherwise

        Returns:
            dict: the full data
        """
        if typee == 'artist' or typee == 'song':
            assert str(query).isdecimal()
            url = '{0}s/{1}'.format(typee, query)
            request = self.__get_json(url)
            data = request['response'][typee]
        else:
            url = 'search?access_token={0}&q={1}'.format(self.access_token, query)
            request = self.__get_json(url)
            data = request['response']['hits']
        return data


    def get_lyrics(self, song_id, url=None):
        """
            Retrieves the lyrics of a given song id 

        Args:
            song_id (Int) : id of the songs to get its lyrics
            url (string, optional): url of the song. Defaults to None.

        Returns:
            string: lyrics of the song
        """
        
        if not url:
            path = self.search(song_id, 'song')['path']
            url = "http://genius.com" + path
        page = requests.get(url)
        # Extract the page's HTML as a string
        html = BeautifulSoup(page.text, "html.parser")
        # Scrape the song lyrics from the HTML
        lyric_tag = html.find("div", class_="lyrics")
        if lyric_tag is None:
            class_matcher = re.compile("^Lyrics__Container")
            lyric_tags = html.find_all("div", class_=class_matcher)
            if not lyric_tags: 
                print('no lyrics')
                return ''
            lyric = ''
            for tag in lyric_tags:
                tag = re.sub('>','>\\n',str(tag))
                lyric = lyric + re.sub('<.*>','',str(tag))
            lyric = re.sub('\n+','\n',lyric)
            return lyric
        else:
            lyric = lyric_tag.get_text()
            return lyric.strip()


    def get_artist_songs_id(self, artist_id, artist_name=None):
        """
        Retrieve all the songs IDs of an artist

        Args:
            artist_id (Int): THe artist's ID

        Returns:
            List: all the songs that the artist is the primary one
        """
        #Get all the song id from an artist.#
        current_page = 1
        next_page = True
        songs = []  # to store final song ids
        if artist_name:
            print(f'Collecting songs ids of {artist_name}')
        while next_page:
            path = "artists/{}/songs/".format(artist_id)
            params = {'page': current_page}  # the current page
            data = self.__get_json(path=path, params=params)  # get json of songs
            page_songs = data['response']['songs']
            if page_songs:
                songs += page_songs
                current_page += 1
            else:
                next_page = False
        if artist_name:
            print("Song id were scraped from {} pages of artist {}"
                .format(current_page, artist_name))
        else:
            print("Song id were scraped from {} pages of unkown artist"
                .format(current_page))
        songs = [song["id"] for song in songs]

        return songs


if __name__ == "__main__":
    
    print('yo')
