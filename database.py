from peewee import MySQLDatabase, Field, Model, CharField, DecimalField, DateTimeField, IntegerField, BigIntegerField

from credentials import MYSQL_USER, MYSQL_PASS, MYSQL_HOST

DB_NAME = 'twitter_scrape'


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
	text = CharField(max_length=255)
	coordinatesLat = DecimalField(max_digits=9, decimal_places=6)   # Range: (-90, 90)
	coordinatesLong = DecimalField(max_digits=9, decimal_places=6)  # Range: (-180, 180)
	created_at = DateTimeField(formats='%Y-%m-%d %H:%M:%S')
	#TODO: entities
	favorite_count = IntegerField()
	filter_level = TinyIntField()  # Values may be one of ["none", "low", "medium", "high"]
	tweet_id = BigIntegerField()
	place_id = CharField(max_length=255)
	retweet_count = IntegerField()
	retweeted_status_id = BigIntegerField()
	source = CharField(max_length=255)
	user_id = BigIntegerField()


# Handles all database operations
class DB:
	def connect(self):
		database.connect()

	# simple utility function to create tables
	def create_tables(self):
		Tweet.create_table()

	def add_tweet(self, tweetData):
		tweet = Tweet()
		tweet.text = tweetData['text']

		tweet.save()


if __name__ == '__main__':
	db = DB()
	db.connect()
	# db.create_tables()
