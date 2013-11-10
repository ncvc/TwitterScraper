from twython import Twython, TwythonStreamer
import time
import sys

from keys import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET

class MyStreamer(TwythonStreamer):
	def reset_total(self):
		self.last_time = time.clock()
		self.total = 0

	def on_success(self, data):
		self.total += 1
		# print data
		# raise Exception()
		if self.total % 100 == 0:
			print self.total
			print time.clock() - self.last_time
			self.last_time = time.clock()
		# if 'text' in data:
		#     print data['text'].encode('utf-8')
		#     print data

	def on_error(self, status_code, data):
		print status_code

		# Want to stop trying to get data because of the error?
		# Uncomment the next line!
		# self.disconnect()

stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream.reset_total()
stream.statuses.sample()
