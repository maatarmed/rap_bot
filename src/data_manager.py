from pymongo import MongoClient, errors
from genius_api import GeniusApiManager
from threading import Thread
from api_access import *


# DB connexion port
PORT = 27017

class GeniusApiDatabaseManager:
	def __init__(self, port=27017, db_name='GeniusScrappingProject'):
		self.client = MongoClient(port=port)
		self.db = self.client[db_name]

	# Put Requests

	#add new artists to DB
	def __add_artist(self, artist, genius_api):
		"""
		Private method used
		Adds an artist as a document to the MongoDB into the collection artists

		Args:
			artist (dict): artist data {id, name, is_verified, url, songs}
			genius_api (GeniusApiManager): api manager

		"""
		entry = {
			'id' : int(artist['id']),
			'name' : artist['name'].lower(),
			'is_verified' : artist['is_verified'],
			'url' : artist['url'],
			'songs' : genius_api.get_artist_songs_id(artist['id'], artist_name=artist['name'])
			}
				#Step 3: Insert Artist into MongoDB via isnert_one
		self.db.artists.insert_one(entry)

	def add_artists(self, artists, genius_api,nthreads=0):
		"""adds a list of artists ids into the database, you can use threads to add artists into the db 

		Args:
			artists (list(int) / int): the list of artists ids to add to the db. You can add one artist
			genius_api (GeniusApiManager): api manager
			nthreads (int, optional): number of threads to use. Defaults to 0.
		"""
		if isinstance(artists, list):
			print(f'length of list artists {len(artists)}')
			if nthreads <2:
				for artist_id in artists:
					artist = genius_api.search(artist_id)
					self.__add_artist(artist, genius_api)
					print('artists {} added with success'.format(artist['name']))
			elif nthreads > 1:
				threads=[]
				scrapping_batch_size = len(artists) // nthreads
				print(f'thread list size = {scrapping_batch_size}')
				for i in range(nthreads):
					threads.append(Thread(target=self.add_artists, 
						args=(artists[scrapping_batch_size * i : scrapping_batch_size * (i + 1)], genius_api,)))
					if i == threads - 1:
						threads[i] = Thread(self.add_artists, (artists[scrapping_batch_size * i:], genius_api,))
					threads[i].start()
					print('thread {} activated'.format(i+1))
		else:
			artist = genius_api.search(artists)
			self.__add_artist(artist, genius_api)
			print('artists {} added with success'.format(artist['name']))

	def __add_song(self, song, genius_api):
		"""adds a song into the collection songs in the db.

		Args:
			song (dict()): a dictionnary containing the song data {id, title, primary_artist, url, lyrics, album, release_data, featured artists}
			genius_api (GeniusApiManager): api manager
		"""
		entry = {
			'id' : int(song['id']),
			'title' : song['title'],
			'primary_artist' : {
				'id' : song['primary_artist']['id'],
				'name' : str(song['primary_artist']['name']).lower(),
				'url' : song['primary_artist']['url'],
				'is_verified' : song['primary_artist']['is_verified'],
				},
			'url' : song['url'],
			'lyrics' : genius_api.get_lyrics(song['id'], song['url'])
			}
		if song['album']:
			entry['album'] = {
				'id': song['album']['id'], 
				'full_title': song['album']['full_title'], 
				'name': song['album']['name'], 
				'artist': song['album']['artist']['id']
				}
		if song['release_date']:
			entry['release_date'] = song['release_date']
		if len(song['featured_artists']) > 0:
			featured_artists = list()
			for artist in song['featured_artists']:
				art = {
					'id' : artist['id'],
					'name' : artist['name'].lower()
					}
				featured_artists.append(art)
			entry['featured_artists'] = featured_artists
			#Step 3: Insert Artist into MongoDB via isnert_one
		self.db.songs.insert_one(entry)

	def add_songs(self, songs, genius_api, nthreads=0):
		"""adds a list of songs or a song to the DB. you can use threads.

		Args:
			songs (list(int) / int): songs ids to add into the songs collection
			genius_api (GeniusApiManager): api manager
			nthreads (int, optional): number of threads to use. Defaults to 0.
		"""
		if isinstance(songs, list):
			print(f'length of list songs {len(songs)}')
			if nthreads <2:
				for song_id in songs:
					#print(song_id)
					song = genius_api.search(song_id, 'song')
					self.__add_song(song, genius_api)
					#print('songs {} added with success'.format(song['title']))
			elif nthreads >1:
				assert len(songs) > 0
				threads=[]
				scrapping_batch_size = len(songs) // nthreads
				print(f'thread list size = {scrapping_batch_size}')
				for i in range(nthreads):
					threads.append(Thread(target=self.add_songs, 
						args=(songs[scrapping_batch_size * i : scrapping_batch_size * (i + 1)], genius_api,)))
					if i == threads - 1:
						threads[i] = Thread(self.add_songs, (songs[scrapping_batch_size * i:], genius_api,))
					threads[i].start()
					print('thread {} activated'.format(i+1))
		else:
			song = genius_api.search(songs, 'song')
			self.__add_song(song, genius_api)
			print('song {} added with success'.format(song['title']))

	def __add_lyric(self, song, genius_api):
		"""adds a song lyrics into the lyrics collection

		Args:
			song (dict()): the song data {song_id, song_title, song_url, lyrics}
			genius_api (GeniusApiManager): api manager
		"""
		entry = {
			'song_id' : int(song['id']),
			'song_title' : song['title'],
			'url' : song['url']
			}
		try:
			entry['lyrics'] = genius_api.get_lyrics(song['id'], song['url'])
		except:
			entry['lyrics'] = ''	
				#Step 3: Insert Artist into MongoDB via isnert_one
		try:
			self.db.lyrics.insert_one(entry)
		except errors.DuplicateKeyError:
			pass
		
	def add_lyrics(self, songs, genius_api, nthreads=0):
		"""adds a list of songs lyrics into the lyrics collection using __add_lyrics() function

		Args:
			songs (list(int) / int): songs ids
			genius_api (GeniusApiManager): api manager
			nthreads (int, optional): number of threads to use. Defaults to 0.
		"""
		if isinstance(songs, list):
			print(f'{len(songs)} songs to get their lyrics')
			if nthreads <2:
				for song_id in songs:
					song = genius_api.search(song_id, 'song')
					self.__add_lyric(song, genius_api)
			elif nthreads >1:
				assert len(songs) > 0
				threads=[]
				scrapping_batch_size = len(songs) // nthreads
				print(f'thread list size = {scrapping_batch_size}')
				for i in range(nthreads):
					threads.append(Thread(target=self.add_lyrics, 
						args=(songs[scrapping_batch_size * i : scrapping_batch_size * (i + 1)], genius_api,)))
					if i == threads - 1:
						threads[i] = Thread(self.add_lyrics, (songs[scrapping_batch_size * i:], genius_api,))
					threads[i].start()
					print('thread {} activated'.format(i+1))
		else:
			song = genius_api.search(songs, 'song')
			self.__add_lyric(song, genius_api)
			print(' lyrics of {} added with success'.format(song['title']))


	# Get Requests
	def get_songs_of_artist(self, artist_id: int):
		"""Gets the list of songs of an artist from the artist document

		Args:
			artist_id (int): The artist id to get songs

		Returns:
			list(int): songs ids of the artist
		"""
		artist = self.db.artists.find_one({'id': artist_id})
		return artist['songs']

	def get_songs_of_all_artists(self):
		"""Gets all the songs ids of the artists saved in the artists collection

		Returns:
			list(int): songs ids
		"""
		artists = self.db.artists.find()
		all_songs = []
		for artist in artists:
			all_songs.extend(artist['songs'])
		all_songs = list(set(all_songs))
		return all_songs

	def get_existing_songs(self):
		"""Gets the list of existing songs in the songs collection

		Returns:
			list(int): songs ids
		"""
		songs = self.db.songs.find()
		existing_songs = []
		for song in songs:
			existing_songs.append(song['id'])
		return existing_songs

	def get_existing_artists(self):
		"""Gets the list of existing artists from arists collection

		Returns:
			list(int): artists ids
		"""
		artists = self.db.artists.find()
		ids = []
		for artist in artists:
			ids.append(artist['id'])
		return ids

	def get_primary_artists_from_songs(self, songs=None):
		"""Gets the primary artist of the existing songs in the songs collection

		Args:
			songs (list(int), optional): songs ids. Defaults to None.

		Returns:
			list(int): artists ids
		"""
		#Returns the primary artist of all the songs in the DB#
		if not songs:
			songs = self.db.songs.find()
		existing_artists = []
		for _ , song in enumerate(songs):
			if song['primary_artist']['id'] not in existing_artists:
				existing_artists.append(song['primary_artist']['id'])
		return existing_artists

	def get_non_existing_songs_of_artists(self, artists_songs=None, existing_songs=None):
		"""Gets the songs of artists missing in the songs collection

		Args:
			artists_songs (list(int), optional): artists ids. Defaults to None.
			existing_songs (list(int), optional): existing songs ids. Defaults to None.

		Returns:
			list(int): smissing songs ids
		"""
		if not isinstance(artists_songs, list):
			print(1)
			artists_songs = self.get_songs_of_all_artists()
		if not isinstance(existing_songs, list):
			print(2)
			existing_songs = self.get_existing_songs()
		non_existing_songs = [song for i, song in enumerate(artists_songs) 
			if song not in existing_songs]
		return non_existing_songs

	def get_artists_from_songs(self):
		"""Gets the artists ids from the primary and featured artists fields of the existing songs

		Returns:
			list(int): artists ids
		"""
		songs = self.db.songs.find({ "featured_artists": { "$exists": "true" }})
		artists_from_songs = self.get_primary_artists_from_songs(songs)
		for _ , song in enumerate(songs):
			for artist in song["featured_artists"]:
				if artist['id'] not in artists_from_songs:
					print(artist['name'])
					artists_from_songs.extend(artist['id'])
		return artists_from_songs

	def get_existing_lyrics(self, song_id):
		"""Gets the lyrics of a song

		Args:
			song_id (int): song id

		Returns:
			string: Lyrics of the song
		"""
		lyrics = self.db.lyrics.find_one({'song_id': song_id})['lyrics']
		return lyrics

	def get_existing_lyrics_of_artist(self, artist_name=None, artist_id=None):
		"""Gets the lyrics of all songs of an artist

		Args:
			artist_name (string, optional): artist name. Defaults to None.
			artist_id (int, optional): artist id. Defaults to None.

		Returns:
			list(Tuple(int,string)): song id and lyrics of all songs
		"""
		if artist_name:
			songs = self.db.artists.find_one({'name': str(artist_name).lower()})
			lyrics = []
			for song in songs:
				lyrics.append((song, self.get_existing_lyrics(song)))
			return lyrics
		if artist_id:
			songs = self.db.artists.find_one({'id': artist_id})['songs']
			print(len(songs))
			lyrics = []
			for song in songs:
				try:
					lyrics.append((song, self.get_existing_lyrics(song)))
				except:
					continue
			return lyrics




if __name__ == "__main__":
	genius_api_manager = GeniusApiManager(API_CLIENT_ACCESS_TOKEN)
	dbm = GeniusApiDatabaseManager(PORT)
	#existing_songs = dbm.get_existing_songs()
	#print(existing_songs[:10])
	#print(len(dbm.get_existing_lyrics_of_artist(artist_id=45)))
	dbm.add_artists(45,genius_api_manager)
	
	#add_artists(non_existing_artists, 4)