from pymongo import MongoClient
import json
from genius_api import search, get_artist_songs_id
from tqdm import tqdm
from threading import Thread


# DB connexion
global client
client = MongoClient(port=27017)
global db
db = client.GeniusScrappingProject


# Put Requests

#add new artists to our DB
def add_artist(artist):
	entry = {
		'id' : int(artist['id']),
		'name' : artist['name'],
		'is_verified' : artist['is_verified'],
		'url' : artist['url'],
		'songs' : get_artist_songs_id(artist['id'], artist_name=artist['name'])
		}
			#Step 3: Insert Artist into MongoDB via isnert_one
	db.artists.insert_one(entry)

def add_artists(artists, nthreads=0):
	if isinstance(artists, list):
		print(f'length of list artists {len(artists)}')
		if nthreads <2:
			for artist_id in artists:
				artist = search(artist_id)
				add_artist(artist)
				print('artists {} added with success'.format(artist['name']))
		elif nthreads > 1:
			threads=[]
			scrapping_batch_size = len(artists) // nthreads
			print(f'thread list size = {scrapping_batch_size}')
			for i in range(nthreads):
				threads.append(Thread(target=add_artists, 
					args=(artists[scrapping_batch_size * i : scrapping_batch_size * (i + 1)],)))
				if i == len(artists)-1:
					threads.append(Thread(add_artists, (artists[scrapping_batch_size * i:],)))
				threads[i].start()
				print('thread {} activated'.format(i+1))
	else:
		artist = search(artists)
		add_artist(artist)
		print('artists {} added with success'.format(artist['name']))

def add_song(song):
	entry = {
		'id' : int(song['id']),
		'title' : song['title'],
		'primary_artist' : {
			'id' : song['primary_artist']['id'],
			'name' : song['primary_artist']['name'],
			'url' : song['primary_artist']['url'],
			'is_verified' : song['primary_artist']['is_verified']
			},
		'url' : song['url']
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
				'name' : artist['name']
				}
			featured_artists.append(art)
		entry['featured_artists'] = featured_artists
		#Step 3: Insert Artist into MongoDB via isnert_one
	db.songs.insert_one(entry)

def add_songs(songs, nthreads=0):
	if isinstance(songs, list):
		print(f'length of list songs {len(songs)}')
		if nthreads <2:
			for song_id in songs:
				#print(song_id)
				song = search(song_id, 'song')
				add_song(song)
				#print('songs {} added with success'.format(song['title']))
		elif nthreads >1:
			assert len(songs) > 0
			threads=[]
			scrapping_batch_size = len(songs) // nthreads
			print(f'thread list size = {scrapping_batch_size}')
			for i in range(nthreads):
				threads.append(Thread(target=add_songs, 
					args=(songs[scrapping_batch_size * i : scrapping_batch_size * (i + 1)],)))
				if i == len(songs)-1:
					threads.append(Thread(add_songs, (songs[scrapping_batch_size * i:],)))
				threads[i].start()
				print('thread {} activated'.format(i+1))
	else:
		song = search(songs, 'song')
		add_song(song)
		print('song {} added with success'.format(song['title']))
# Get Requests
def get_songs_of_artist(artist_id: int):
	artist = db.artists.find_one({'id': artist_id})
	return artist['songs']

def get_songs_of_all_artists():
	artists = db.artists.find()
	all_songs = []
	for artist in tqdm(artists):
		all_songs.extend(artist['songs'])
	all_songs = list(set(all_songs))
	return all_songs
def get_existing_songs():
	#Returns a list of the songs and their primary artists#
	songs = db.songs.find()
	existing_songs = []
	for song in tqdm(songs):
		existing_songs.append(song['id'])
	return existing_songs

def get_existing_artists():
	artists = db.artists.find()
	ids = []
	for artist in artists:
		ids.append(artist['id'])
	return ids

def get_primary_artists_from_songs(songs=None):
	#Returns the primary artist of all the songs in the DB#
	if not songs:
		songs = db.songs.find()
	existing_artists = []
	for _ , song in enumerate(songs):
		if song['primary_artist']['id'] not in existing_artists:
			existing_artists.append(song['primary_artist']['id'])
	return existing_artists

def non_existing_songs_of_artists(artists_songs=None, existing_songs=None):
	if not isinstance(artists_songs, list):
		print(1)
		artists_songs = get_songs_of_all_artists()
	if not isinstance(existing_songs, list):
		print(2)
		existing_songs = get_existing_songs()
	non_existing_songs = [song for i, song in enumerate(artists_songs) 
		if song not in existing_songs]
	return non_existing_songs

def get_artists_from_songs():
	songs = db.songs.find({ "featured_artists": { "$exists": "true" }})
	existing_artists = get_primary_artists_from_songs(songs)
	for _ , song in enumerate(songs):
		for artist in song["featured_artists"]:
			if artist['id'] not in existing_artists:
				print(artist['name'])
				existing_artists.extend(artist['id'])
	return existing_artists


if __name__ == "__main__":
	songs = get_songs_of_all_artists()
	existing_songs = get_existing_songs()
	non_existing_songs = non_existing_songs_of_artists(artists_songs=songs, 
		existing_songs=existing_songs)
	print(len(songs))
	#add_songs(non_existing_songs, 4)
	all_artists = get_artists_from_songs()
	existing_artists = get_existing_artists()
	print(len(all_artists), len(existing_artists))
	non_existing_artists = [artist for artist in all_artists 
		if artist not in existing_artists]

	add_artists(non_existing_artists, 4)