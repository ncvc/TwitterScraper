import time
from HTMLParser import HTMLParser

from peewee import MySQLDatabase, Field, Model, CharField, DecimalField, DateTimeField, IntegerField, BigIntegerField

from credentials import MYSQL_USER, MYSQL_PASS, MYSQL_HOST


DB_NAME = 'twitter'
FILTER_LEVEL = {None: None, 'none': 0, 'low': 1, 'medium': 2, 'high': 3}


html_parser = HTMLParser()

MySQLDatabase.register_fields({'TINYINT': 'TINYINT'})

database = MySQLDatabase(DB_NAME, host=MYSQL_HOST, port=3306, user=MYSQL_USER, passwd=MYSQL_PASS)


# Custom Field definitions
class TinyIntField(Field):
	db_field = 'TINYINT'


# Model definitions
class BaseModel(Model):
	class Meta:
		database = database

# Should be ~400 bytes max
class Tweet(BaseModel):
	text = CharField(max_length=255, null=True)
	coordinatesLong = DecimalField(max_digits=9, decimal_places=6, null=True)  # Range: (-180, 180)
	coordinatesLat = DecimalField(max_digits=9, decimal_places=6, null=True)   # Range: (-90, 90)
	created_at = DateTimeField(formats='%Y-%m-%d %H:%M:%S', null=True)
	#TODO: entities
	favorite_count = IntegerField(null=True)
	filter_level = TinyIntField(null=True)  # Values may be one of ["none", "low", "medium", "high"]
	tweet_id = BigIntegerField(null=True)
	place_id = CharField(max_length=255, null=True)
	retweet_count = IntegerField(null=True)
	retweeted_status_id = BigIntegerField(null=True)
	source = CharField(max_length=255, null=True)
	user_id = BigIntegerField(null=True)


# Handles all database operations
class DB:
	def connect(self):
		database.connect()
		database.execute_sql('SET NAMES utf8mb4;')  # Necessary for some emojis

	def close(self):
		database.close()

	# Simple utility function to create tables
	def create_tables(self):
		Tweet.create_table()

	# Adds a Queue of tweets to the db (more efficient than calling add_tweet repeatedly)
	def add_tweets(self, tweets, logger):
		with database.transaction():
			logger.info('init transaction')
			while not tweets.empty():
				logger.info('tweet!')
				tweet = tweets.get()
				try:
					self.add_tweet(tweet)
				except:
					logger.exception('db add_tweet exception %s' % str(tweet))
			logger.info('end transaction')


	# Adds the tweet data to the db
	def add_tweet(self, tweetData):
		tweet = Tweet()

		# Populate the tweet. Use the get method to ensure no KeyErrors are raised
		text = tweetData.get('text')
		if text == None:
			tweet.text = None
		else:
			tweet.text = html_parser.unescape(text)

		coords = tweetData.get('coordinates')
		if coords == None or coords == 'None':
			tweet.coordinatesLong = None
			tweet.coordinatesLat = None
		else:
			tweet.coordinatesLong = coords.get('coordinates')[0]
			tweet.coordinatesLat = coords.get('coordinates')[1]

		created_at = tweetData.get('created_at')
		if created_at == None:
			tweet.created_at = None
		else:
			t = time.strptime(created_at.replace('+0000', ''), '%a %b %d %H:%M:%S %Y')
			tweet.created_at = time.strftime('%Y-%m-%d %H:%M:%S', t)

		tweet.favorite_count = tweetData.get('favorite_count')

		tweet.filter_level = FILTER_LEVEL[tweetData.get('filter_level')]

		tweet.tweet_id = tweetData.get('id')

		place = tweetData.get('place')
		if place == None or place == 'None':
			tweet.place_id = None
		else:
			tweet.place_id = place.get('id')

		tweet.retweet_count = tweetData.get('retweet_count')

		retweeted_status = tweetData.get('retweeted_status')
		if retweeted_status == None or retweeted_status == 'None':
			tweet.retweeted_status_id = None
		else:
			tweet.retweeted_status_id = retweeted_status.get('id')

		tweet.source = tweetData.get('source')

		user = tweetData.get('user')
		if user == None:
			tweet.user_id = None
		else:
			tweet.user_id = user.get('id')

		# Write the new row to the database
		tweet.save()


if __name__ == '__main__':
	db = DB()
	db.connect()
	db.create_tables()
	db.close()
