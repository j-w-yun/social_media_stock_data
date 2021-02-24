# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import csv
import json
import os
import pandas as pd
import requests
import sys
import threading
import twint
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from queue import Queue

import time
from stem import Signal
from stem.control import Controller


NUM_WORKERS = 32
SYMBOL_TABLE = 'symbol_data/symbol_table.csv'

def get_tor_session():
	"""Use the tor network as a proxy.
	"""
	session = requests.session()
	session.proxies = {
		'http': 'socks5://127.0.0.1:9050',
		'https': 'socks5://127.0.0.1:9050',
	}
	return session

def renew_connection():
	"""Establish a clean pathway through the tor network.
	"""
	with Controller.from_port(port=9051) as c:
		password = os.environ.get('TOR_CONTROLLER_PW')
		c.authenticate(password=password)
		c.signal(Signal.NEWNYM)

def get_symbols():
	"""Get all symbols.
	"""
	symbols = []
	with open(SYMBOL_TABLE, 'r', encoding='utf-8') as f:
		dw = csv.DictReader(f, delimiter='|')
		for row in dw:
			symbol = row
			symbol['symbol'] = symbol['symbol'].strip()
			symbols.append(symbol)
	return sorted(symbols, key=lambda k: k['symbol'])

def sanitize(s, delimiter='|'):
	"""Sanitize whitespace and delimiter.
	"""
	res = s
	res = res.replace(delimiter, ',')
	res = ' '.join(res.split())
	return res

def fs_encode(symbol):
	"""Encode symbol into filesystem-safe string.
	"""
	res = symbol
	res = res.replace('.', '_')
	res = res.replace('/', '-')
	return res

class TWITTER:
	def __init__(self, directory):
		self.directory = directory

	def get_filename(self, symbol):
		output_dir = os.path.join(self.directory, fs_encode(symbol))
		return os.path.join(output_dir, 'tweets.csv')

	def get_last_date(self, filename):
		"""Get latest time from data.
		"""
		data = pd.read_csv(filename, sep=',')
		data['new_date'] = data['date'] + ' ' + data['time']
		data['new_date'] = pd.to_datetime(data['new_date'], format='%Y-%m-%d %H:%M:%S')
		return data['new_date'].max()

	def _download_tweets(self, symbol, start_date, end_date):
		filename = self.get_filename(symbol['symbol'])
		output_dir = os.path.join(*filename.split(os.path.sep)[:-1])

		# Update from last date
		last_date = start_date
		if os.path.isfile(filename):
			last_date = self.get_last_date(filename)

		# Adjust PST to UTC
		since = last_date + timedelta(hours=8, seconds=1)
		since = since.strftime('%Y-%m-%d %H:%M:%S')
		until = end_date + timedelta(hours=8)
		until = until.strftime('%Y-%m-%d %H:%M:%S')
		print('Twitter start {} {}'.format(symbol['symbol'], since))

		# Get data
		c = twint.Config()
		c.Search = '${}'.format(symbol['symbol'])
		c.Since = since
		c.Until = until
		c.Output = output_dir
		c.Lang = 'en'
		c.Count = False
		c.Stats = False
		c.Min_retweets = 1
		c.Hide_output = True
		c.Store_csv = True
		c.Proxy_host = 'tor'
		c.Tor_control_port = 9051
		c.Tor_control_password = os.environ.get('TOR_CONTROLLER_PW')
		twint.run.Search(c)

		print('Twitter done {} {}'.format(symbol['symbol'], until))

	def download_tweets(self, symbol):
		last_date = datetime.strptime('2011-03-01', '%Y-%m-%d')
		# now = datetime.strptime('2011-12-01', '%Y-%m-%d')
		now = datetime.now()
		self._download_tweets(symbol, last_date, now)

	def work(self, jobs):
		while not jobs.empty():
			kwargs = jobs.get()
			self.download_tweets(**kwargs)
			jobs.task_done()

	def update(self, use_threads=True):
		"""Warning: using threads might cross the rate limit and get you banned.
		"""
		symbols = get_symbols()
		if use_threads:
			jobs = Queue()
			for symbol in symbols:
				jobs.put({'symbol':symbol})
			for _ in range(NUM_WORKERS):
				worker = threading.Thread(target=self.work, args=[jobs])
				worker.start()
			jobs.join()
		else:
			for symbol in symbols:
				self.download_tweets(symbol)
		print('Twitter update complete')

	def get_data(self):
		data = {}
		symbols = get_symbols()
		for symbol in symbols:
			filename = self.get_filename(symbol['symbol'])
			if os.path.isfile(filename):
				data[symbol['symbol']] = pd.read_csv(filename, sep=',')
				print('Read csv {}'.format(symbol['symbol']))
			else:
				print('No data {}'.format(symbol['symbol']))
		return data

class REDDIT:
	def __init__(self, directory, subreddit, delimiter='|'):
		self.subreddit = subreddit
		self.directory = directory
		self.delimiter = delimiter
		self.url = 'https://api.pushshift.io/reddit/search/{}'
		self.comment_fieldnames = ['created_utc', 'all_awardings', 'associated_award', 'author', 'author_cakeday', 'author_created_utc', 'author_flair_background_color', 'author_flair_css_class', 'author_flair_richtext', 'author_flair_template_id', 'author_flair_text', 'author_flair_text_color', 'author_flair_type', 'author_fullname', 'author_patreon_flair', 'author_premium', 'awarders', 'body', 'can_gild', 'collapsed', 'collapsed_because_crowd_control', 'collapsed_reason', 'comment_type', 'controversiality', 'distinguished', 'edited', 'gilded', 'gildings', 'id', 'is_submitter', 'link_id', 'locked', 'media_metadata', 'mod_removed', 'no_follow', 'nest_level', 'parent_id', 'permalink', 'permalink_url', 'reply_delay', 'retrieved_on', 'score', 'score_hidden', 'send_replies', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_type', 'top_awarded_type', 'total_awards_received', 'treatment_tags', 'updated_utc', 'user_removed',]
		self.submission_fieldnames = ['created_utc', 'all_awardings', 'allow_live_comments', 'approved_at_utc', 'archived', 'author', 'author_cakeday', 'author_created_utc', 'author_flair_background_color', 'author_flair_css_class', 'author_flair_richtext', 'author_flair_template_id', 'author_flair_text', 'author_flair_text_color', 'author_flair_type', 'author_fullname', 'author_id', 'author_patreon_flair', 'author_premium', 'awarders', 'banned_at_utc', 'banned_by', 'brand_safe', 'can_gild', 'can_mod_post', 'category', 'content_categories', 'contest_mode', 'crosspost_parent', 'crosspost_parent_list', 'distinguished', 'domain', 'edited', 'full_link', 'gallery_data', 'gilded', 'gildings', 'hidden', 'id', 'is_crosspostable', 'is_gallery', 'is_meta', 'is_original_content', 'is_reddit_media_domain', 'is_robot_indexable', 'is_self', 'is_video', 'link_flair_background_color', 'link_flair_css_class', 'link_flair_richtext', 'link_flair_template_id', 'link_flair_text', 'link_flair_text_color', 'link_flair_type', 'locked', 'media', 'media_embed', 'media_metadata', 'media_only', 'mod_reports', 'no_follow', 'num_comments', 'num_crossposts', 'over_18', 'parent_whitelist_status', 'permalink', 'pinned', 'post_hint', 'preview', 'previous_visits', 'pwls', 'quarantine', 'removal_reason', 'removed_by_category', 'retrieved_on', 'rte_mode', 'score', 'secure_media', 'secure_media_embed', 'selftext', 'send_replies', 'spoiler', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_name_prefixed', 'suggested_sort', 'subreddit_subscribers', 'subreddit_type', 'thumbnail', 'thumbnail_height', 'thumbnail_width', 'treatment_tags', 'title', 'total_awards_received', 'updated_utc', 'upvote_ratio', 'url', 'url_overridden_by_dest', 'user_reports', 'view_count', 'whitelist_status', 'wls',]

	def get_filename(self, symbol, post_type):
		output_dir = os.path.join(self.directory, fs_encode(symbol))
		return os.path.join(output_dir, '{}_{}.csv'.format(self.subreddit, post_type))

	def get_last_time(self, filename):
		"""Get latest time from CSV.
		"""
		last_line = ''
		with open(filename, 'r') as f:
			f.seek(0, 2)
			fsize = f.tell()
			f.seek(max (fsize-4096*64, 0), 0)
			lines = f.read().splitlines()
			if len(lines) <= 1:
				return 0
			last_line = lines[-1]
		return last_line.split(self.delimiter)[0]

	def save_data(self, data, filename, post_type):
		"""Append data to csv.
		"""
		file_exists = os.path.exists(filename)

		# Create data path
		path = filename.split(os.path.sep)
		cur_path = ''
		for p in path[:-1]:
			cur_path = os.path.join(cur_path, p)
			if not os.path.exists(cur_path):
				os.makedirs(cur_path)

		with open(filename, 'a', encoding='utf-8') as f:
			fieldnames = []
			if post_type == 'submission':
				fieldnames = self.submission_fieldnames
			elif post_type == 'comment':
				fieldnames = self.comment_fieldnames

			dw = csv.DictWriter(f, delimiter=self.delimiter, extrasaction='ignore', fieldnames=fieldnames)
			# dw = csv.DictWriter(f, delimiter=self.delimiter, fieldnames=self.comment_fieldnames)
			if not file_exists:
				dw.writeheader()
			for datum in data:
				dw.writerow(datum)

	def _download_data(self, symbol, post_type, start_time=0):
		query = symbol['symbol']
		if len(query) < 3:
			query = symbol['shortName'].split(' ')[:-1]
			query = ' '.join(query).replace(',', '')

		# Request
		params = {
			'subreddit': self.subreddit,
			'size': 500,
			'sort': 'asc',
			'sort_type': 'created_utc',
			'after': start_time,
			'score': '>1',
			'q': query,
		}
		# url = self.url.format(post_type)
		# res = requests.get(url, params=params)
		# if res.status_code != 200:
		# 	return None

		renew_connection()
		session = get_tor_session()
		time.sleep(1)
		url = self.url.format(post_type)
		res = session.get(url, params=params)
		if res.status_code != 200:
			return None

		# Data is a list of dicts
		data = res.json()['data']

		# Sanitize values for csv
		for i in range(len(data)):
			for k in data[i]:
				if isinstance(data[i][k], str):
					data[i][k] = sanitize(data[i][k], self.delimiter)
				else:
					dump = json.dumps(data[i][k])
					data[i][k] = json.loads(sanitize(dump, self.delimiter))

		# Append data to csv
		filename = self.get_filename(symbol['symbol'], post_type)
		self.save_data(data, filename, post_type)

		return data

	def download_data(self, symbol, post_type):
		# Start from last time in CSV
		filename = self.get_filename(symbol['symbol'], post_type)
		last_time = 0;
		if os.path.isfile(filename):
			last_time = self.get_last_time(filename)
		print('Reddit start {} {} {}'.format(symbol['symbol'], post_type, last_time))

		# Run until CSV is up-to-date
		while True:
			# Get data
			data = self._download_data(symbol, post_type, last_time)

			# Data is none if request failed to fetch data
			if data is None:
				continue

			# CSV is up-to-date
			if len(data) == 0:
				print('Reddit done {} {} {}'.format(symbol['symbol'], post_type, last_time))
				break

			# Set latest time
			last_time = data[-1]['created_utc']

			print('Reddit got {} {} {} - {}'.format(
				symbol['symbol'],
				post_type,
				datetime.fromtimestamp(data[0]['created_utc']),
				datetime.fromtimestamp(last_time)))

	def work(self, jobs):
		while not jobs.empty():
			kwargs = jobs.get()
			self.download_data(**kwargs)
			jobs.task_done()

	def update(self):
		jobs = Queue()
		symbols = get_symbols()
		for symbol in symbols:
			jobs.put({'symbol':symbol, 'post_type':'submission'})
			jobs.put({'symbol':symbol, 'post_type':'comment'})
		for _ in range(NUM_WORKERS):
			worker = threading.Thread(target=self.work, args=[jobs])
			worker.start()
		jobs.join()
		print('Reddit update complete')

def update_twitter():
	twitter = TWITTER(directory='twitter_data')
	twitter.update()

def update_reddit():
	reddit = REDDIT(directory='reddit_data', subreddit='wallstreetbets')
	reddit.update()

if __name__ == '__main__':
	opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]

	if "-t" in opts:
		update_twitter()
	elif "-r" in opts:
		update_reddit()
	elif "-a" in opts:
		update_twitter()
		update_reddit()
	else:
		print('Please specify a platform to download.\n' +
			'Twitter: `-t`, Reddit: `-r`, all: `-a`')
