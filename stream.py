import time
from threading import Thread
from Queue import Queue
import logging

from twython import Twython, TwythonStreamer

from database import DB
from credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET


# Make a global logging object.
logit = logging.getLogger("logit")
logit.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s %(asctime)s %(threadName)s:%(funcName)s:%(lineno)d - %(message)s")

# Stream handler
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logit.addHandler(handler)

# File handler
fileHandler = logging.FileHandler("TwitterScraper.log")
fileHandler.setFormatter(formatter)
fileHandler.setLevel(logging.DEBUG)
logit.addHandler(fileHandler)


class MyStreamer(TwythonStreamer):
	def __init__(self, queue, *args, **kwargs):
		super(MyStreamer, self).__init__(*args, **kwargs)
		self.last_time = time.clock()
		self.total = 0
		self.queue = queue
		self.logit = logging.getLogger("logit")

	def on_success(self, data):
		if data != None:
			self.queue.put(data)
		else:
			self.logit.warn('Streamer: data == None. Wat.')

		self.total += 1
		if self.total % 1000 == 0:
			self.logit.info('Total Tweets: %d, elapsed time: %d, queue size: %d' % (self.total, time.clock() - self.last_time, self.queue.qsize()))
			self.last_time = time.clock()

	def on_error(self, status_code, data):
		self.logit.warn('Streaming error: %s, %s' % (str(status_code), str(data)))


# Thread that handles stream reading
class StreamThread(Thread):
	def __init__(self, queue):
		Thread.__init__(self, name='Thread-Stream')
		self.queue = queue
		self.stream = MyStreamer(queue, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

	def disconnect(self):
		self.stream.disconnect()

	def run(self):
		self.stream.statuses.sample(language='en', stall_warnings='true')


# Thread that handles DB writes
class DBThread(Thread):
	def __init__(self, queue):
		Thread.__init__(self, name='Thread-DB')
		self.queue = queue
		self.db = DB()
		self.logit = logging.getLogger("logit")
		self.to_commit = Queue()
		self.commit_size = 1

	def run(self):
		self.db.connect()

		while True:
			tweet = self.queue.get()

			if tweet == None:
				self.logit.info('Terminal sentinel encountered')
				self.queue.task_done()
				break

			if 'delete' in tweet:
				self.logit.info('delete: %s' % str(tweet))
			elif 'scrub_geo' in tweet:
				self.logit.info('scrub_geo: %s' % str(tweet))
			elif 'limit' in tweet:
				self.logit.warning('limit: %s' % str(tweet))
			elif 'status_withheld' in tweet:
				self.logit.info('status_withheld: %s' % str(tweet))
			elif 'user_withheld' in tweet:
				self.logit.info('user_withheld: %s' % str(tweet))
			elif 'disconnect' in tweet:
				self.logit.warning('disconnect: %s' % str(tweet))
				break
			elif 'warning' in tweet:
				self.logit.warning('warning: %s' % str(tweet))
			elif 'id' in tweet and 'text' in tweet:
				try:
					self.db.add_tweet(tweet)
				except:
					self.logit.exception('db add_tweet exception %s' % str(tweet))

				# TODO: Get multi-queries working, so we don't need a super low-latency connection to the database
				# self.to_commit.put(tweet)

				# Commit self.commit_size tweets at once
				# if self.to_commit.qsize() % 100 == 0:
				# 	self.logit.info('to_commit size: %d' % self.to_commit.qsize())

				# if self.to_commit.qsize() >= self.commit_size:
				# 	self.logit.info('start committing')
				# 	self.db.add_tweets(self.to_commit, self.logit)
				# 	self.logit.info('end committing')

			self.queue.task_done()

		self.logit.info('Closing db connection')
		self.db.close()


# Thread used to quit gracefully
class KeyThread(Thread):
	def __init__(self):
		Thread.__init__(self, name='Thread-Keystroke')

	def run(self):
		raw_input('Press enter to end\n')


# Start the Streaming and DB threads
def startThreads():
	queue = Queue()

	# Create threads
	streamingThread = StreamThread(queue)
	dbThread = DBThread(queue)
	keyThread = KeyThread()

	streamingThread.daemon = True
	dbThread.daemon = True
	keyThread.daemon = True

	logit.info('Starting threads')
	streamingThread.start()
	dbThread.start()
	keyThread.start()

	# Wait until user presses ENTER
	keyThread.join()
	logit.info('Shut it down')

	# Shutdown streaming thread
	streamingThread.disconnect()
	streamingThread.join()

	logit.info('Streaming thread stopped, %d items left on the queue' % queue.qsize())

	# Wait for queue to empty
	queue.put(None)  # Put the "terminate" sentinel on the queue
	queue.join()     # Wait until all items on the queue are processed


	logit.info('Streaming queue emptied, waiting for db shutdown')
	dbThread.join()
	
	logit.info('Done!')


if __name__ == '__main__':
	startThreads()
