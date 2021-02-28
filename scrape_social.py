# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import csv
import json
import math
import os
import pandas as pd
import requests
import sys
import threading
import time
import twint
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from nltk.corpus import words as en_words
from stem import Signal
from stem.control import Controller
from queue import Queue


NUM_WORKERS = 64
SYMBOL_TABLE = 'symbol_data/symbol_table.csv'
COMMON_SYMBOLS = ['ALL', 'ANY', 'BIG', 'BRO', 'BUY', 'CALM', 'CAN', 'CAP', 'ECO', 'DIET', 'DIG', 'DIM', 'DOG', 'DROP', 'EAT', 'EDIT', 'FAME', 'FAN', 'FAST', 'FAT', 'FATE', 'FIVE', 'FLOW', 'FOUR', 'FUD', 'FUN', 'GOLD', 'GOOD', 'HEAR', 'HOLD', 'HOME', 'HOPE', 'IT', 'JOB', 'JUST', 'KEY', 'KEYS', 'KNOW', 'LAWS', 'LAZY', 'LIFE', 'LOAN', 'LOVE', 'MOM', 'MOON', 'NEAR', 'NEED', 'NERD', 'NEW', 'NEXT', 'NICE', 'NINE', 'NOW', 'ONE', 'OUT', 'PLAN', 'PLAY', 'PUMP', 'ROLL', 'ROOF', 'ROOT', 'SACH', 'SAFE', 'SAIL', 'SAND', 'SALT', 'SAVE', 'SEE', 'SEED', 'SEEK', 'SIX', 'SNOW', 'SO', 'SUB', 'SUP', 'TELL', 'TEN', 'TRUE', 'TWO', 'UNIT', 'VERY', 'WELL', 'WHEN', 'WOW', 'YELL', 'YOLO']
START_FROM = 'A'

class Tor:
	def __init__(self):
		self.is_tor_renewing = False

	def get_tor_session(self, renew=False):
		"""Use the tor network as a proxy.
		"""
		if renew:
			self.renew_connection()

		while self.is_tor_renewing:
			time.sleep(0.1)

		session = requests.session()
		session.proxies = {
			'http': 'socks5://127.0.0.1:9050',
			'https': 'socks5://127.0.0.1:9050',
		}
		return session

	def renew_connection(self):
		"""Establish a clean pathway through the tor network.
		"""
		while self.is_tor_renewing:
			time.sleep(0.2)

		self.is_tor_renewing = True
		with Controller.from_port(port=9051) as c:
			password = os.environ.get('TOR_CONTROLLER_PW')
			c.authenticate(password=password)
			c.signal(Signal.NEWNYM)
		self.is_tor_renewing = False


tor = Tor()

class Dictionary:
	def __init__(self):
		try:
			self.initialize()
		except LookupError:
			import nltk
			nltk.download('words')
			self.initialize()

	def initialize(self):
		self.lower_en_words = [w.lower() for w in en_words.words()]

	def is_word(self, word):
		"""Check if word exists in the English dictionary.
		"""
		return word.lower() in self.lower_en_words

dictionary = Dictionary()

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
		c.Min_retweets = 1
		c.Hide_output = True
		c.Store_csv = True
		c.Proxy_host = 'tor'
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
				if symbol['symbol'][0] >= START_FROM:
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

	def get_fieldnames(self, post_type):
		if post_type == 'submission':
			return self.submission_fieldnames
		elif post_type == 'comment':
			return self.comment_fieldnames
		else:
			raise Exception('Must provide valid post type.')

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

	def save_data(self, data, filename, fieldnames):
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
			dw = csv.DictWriter(f, delimiter=self.delimiter, extrasaction='ignore', fieldnames=fieldnames)
			# dw = csv.DictWriter(f, delimiter=self.delimiter, fieldnames=self.comment_fieldnames)
			if not file_exists:
				dw.writeheader()
			for datum in data:
				dw.writerow(datum)

	def _to_company_name(self, orig_name):
		name = orig_name.split(',')[0]
		name = name.lower()
		name = name.strip()
		words = []

		break_on_word = False
		split_name = name.split()
		for i, word in enumerate(split_name):
			# Skip articles
			if len(words) == 0 and word in ['a', 'an', 'the']:
				continue
			# Break on English word after observing non-English word
			if not dictionary.is_word(word):
				if len(words) > 0:
					break_on_word = True
			elif break_on_word:
				break
			words.append(word)

		remove_words = [
			'corporation',
			'corp',
			'cor',
			'etf',
			'incorporated',
			'inc',
			'limited',
			'ltd',
		]
		if len(words) > 0:
			last_word = words[-1].replace('.', '')
			if last_word in remove_words or len(last_word) <= 2:
				words = words[:-1]

		# Join name
		name = ' '.join(words)

		# Replace ambiguous name
		if len(name) <= 3:
			name = orig_name.replace(',', '')
			name = orig_name.replace('.', '')
			return name.lower()
		return name

	def _get_query_str(self, symbol):
		queries = []

		long_name = self._to_company_name(symbol['longName'])
		if len(long_name) > 0:
			queries.append(long_name)
		short_name = self._to_company_name(symbol['shortName'])
		if len(short_name) > 0:
			queries.append(short_name)

		# Sort by length
		sorted(queries, key=len)

		# Remove duplicate strings and superstrings
		query_set = []
		for q in queries:
			add_query = True
			for unique in query_set:
				if unique in q:
					add_query = False
			if add_query:
				query_set.append(q)
		return query_set

	def _download_data(self, symbol, post_type, start_time=0, session=None):
		query_set = self._get_query_str(symbol)

		# Symbols with cashtag
		detect_cashtag = False
		if symbol['symbol'] in COMMON_SYMBOLS:
			query_set.insert(0, '${}'.format(symbol['symbol']))
			detect_cashtag = True
		elif len(symbol['symbol']) > 1:
			query_set.insert(0, symbol['symbol'])

		query = '|'.join(query_set)

		# No query
		if len(query) == 0:
			print('\tNo query {}|{}|{}'.format(symbol['symbol'], symbol['shortName'], symbol['longName']))
			return []
		# if post_type == 'comment':
		print('\t{}'.format(query), '==', '{}|{}|{}'.format(symbol['symbol'], symbol['shortName'], symbol['longName']))

		# Request
		params = {
			'subreddit': self.subreddit,
			'size': 500,
			'sort': 'asc',
			'sort_type': 'created_utc',
			'after': start_time,
			'q': query,
			# 'score': '>1',
		}

		if session is None:
			raise Exception('Session is unspecified.')

		url = self.url.format(post_type)
		# res = requests.get(url, params=params)
		res = session.get(url, params=params)
		if res.status_code != 200:
			return None

		# Data is a list of dicts
		data = res.json()['data']
		# print(json.dumps(data[0], indent=4, sort_keys=True))

		# Match symbol cashtag, if text contains it
		if detect_cashtag:
			new_data = []
			for post in data:
				is_valid_post = True
				for attr in ['title', 'selftext', 'body']:
					if attr not in post:
						continue
					# Check if something other than a symbol matched
					matched_query_set = False
					for q in query_set[1:]:
						if q in post[attr].lower():
							matched_query_set = True
							break
					# Check if exact cashtag symbol matches
					if not matched_query_set:
						cashtag = query_set[0].lower()
						post_words = post[attr].lower()
						post_words = post_words.replace(',', ' ')
						post_words = post_words.replace(';', ' ')
						post_words = post_words.replace('.', ' ')
						post_words = post_words.split()
						if symbol['symbol'].lower() in post[attr].lower() and cashtag not in post_words:
							is_valid_post = False
							break
				# Append only valid posts
				if is_valid_post:
					new_data.append(post)
			# Data is non zero but was filtered to zero
			if len(new_data) == 0 and len(data) > 0:
				return data
			# Replace
			data = new_data

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
		fieldnames = self.get_fieldnames(post_type)
		self.save_data(data, filename, fieldnames)

		return data

	def download_data(self, symbol, post_type, worker_id=None, verbose=True):
		session = tor.get_tor_session(renew=True)

		# Start from last time in CSV
		filename = self.get_filename(symbol['symbol'], post_type)
		last_time = 0;
		if os.path.isfile(filename):
			last_time = self.get_last_time(filename)
		if verbose:
			print('{}: Reddit start {} {} {}'.format(
				worker_id,
				symbol['symbol'],
				post_type,
				last_time))

		# Run until CSV is up-to-date
		while True:
			# Get data
			data = self._download_data(symbol, post_type, last_time, session)

			# Data is none if request failed to fetch data
			if data is None:
				continue

			# CSV is up-to-date
			if len(data) == 0:
				if verbose:
					print('{}: Reddit done {} {} {}'.format(
						worker_id,
						symbol['symbol'],
						post_type,
						last_time))
				break

			# Set latest time
			last_time = data[-1]['created_utc']

			if verbose:
				print('{}: Reddit got {} {} {} - {}'.format(
					worker_id,
					symbol['symbol'],
					post_type,
					datetime.fromtimestamp(data[0]['created_utc']),
					datetime.fromtimestamp(last_time)))

	def work(self, jobs, worker_id):
		while not jobs.empty():
			kwargs = jobs.get()
			self.download_data(**kwargs, worker_id=worker_id)
			jobs.task_done()

	def update(self):
		jobs = Queue()
		symbols = get_symbols()
		for symbol in symbols:
			if symbol['symbol'][0] >= START_FROM:
				jobs.put({'symbol':symbol, 'post_type':'submission'})
				jobs.put({'symbol':symbol, 'post_type':'comment'})
		for worker_id in range(NUM_WORKERS):
			worker = threading.Thread(target=self.work, args=[jobs, worker_id])
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
