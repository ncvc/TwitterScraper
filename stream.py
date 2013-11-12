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
formatter = logging.Formatter("%(levelname)s %(asctime)s %(funcName)s:%(lineno)d - %(message)s")

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
	def initialize(self, queue):
		self.last_time = time.clock()
		self.total = 0
		self.queue = queue
		self.logit = logging.getLogger("logit")

	def on_success(self, data):
		if data != None:
			self.queue.put(data, block=False)
		else:
			self.logit.warn('Streamer: data == None. Wat.')

		self.total += 1
		if self.total % 100 == 0:
			self.logit.info('Total Tweets: %d, elapsed time: %d, queue size: %d' % (self.total, time.clock() - self.last_time, self.queue.qsize()))
			self.last_time = time.clock()

	def on_error(self, status_code, data):
		self.logit.warn('Streaming error: %s, %s' % (str(status_code), str(data)))


# Thread that handles stream reading
class StreamThread(Thread):
	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue
		self.stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
		self.stream.initialize(queue)

	def disconnect(self):
		self.stream.disconnect()

	def run(self):
		self.stream.statuses.sample(language='en', stall_warnings='true')


# Thread that handles DB writes
class DBThread(Thread):
	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue
		self.db = DB()
		self.logit = logging.getLogger("logit")

	def closeDB(self):
		self.db.close()

	def run(self):
		self.db.connect()

		while True:
			tweet = self.queue.get()

			if tweet == None:
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
					self.logit.exception('db add_tweet exception')
					self.logit.debug(tweet)

			self.queue.task_done()


def joinWithCtrlC(thread):
	while thread.isAlive():
		thread.join(1000)


# Start the Streaming and DB threads
def startThreads():
	queue = Queue()

	streamingThread = StreamThread(queue)
	dbThread = DBThread(queue)

	streamingThread.daemon = True
	dbThread.daemon = True

	logit.info('Starting threads')
	streamingThread.start()
	dbThread.start()

	try:
		joinWithCtrlC(streamingThread)
		logit.info('Streaming Thread exited')

		queue.put(None)  # Put the "terminate" sentinel on the queue

		joinWithCtrlC(dbThread)
		logit.info('DB Thread exited')
	except KeyboardInterrupt:
		logit.info('Ctrl-c detected, shut it down')

		streamingThread.disconnect()

		logit.info('%d items left on the queue' % queue.qsize())

		queue.put(None)
		queue.join()

		logit.info('Finished the queue, closing db connection')
		dbThread.closeDB()


if __name__ == '__main__':
	startThreads()
