import csv
import json
import os
import random
import pandas as pd
import numpy as np
import sys
from datetime import datetime, timedelta
from textblob import TextBlob

SUBREDDIT = 'wallstreetbets'
REDDIT_DIR = 'reddit_data'
TWITTER_DIR = 'twitter_data'

def fs_encode(symbol):
	"""Encode symbol into filesystem-safe string.
	"""
	res = symbol
	res = res.replace('.', '_')
	res = res.replace('/', '-')
	return res

def get_social_data(symbol):
	symbol = symbol.upper()
	submissions_filename = os.path.join('..', REDDIT_DIR, fs_encode(symbol), '{}_{}.csv'.format(SUBREDDIT, 'submission'))
	submissions_data = pd.read_csv(submissions_filename, sep='|')
	comments_filename = os.path.join('..', REDDIT_DIR, fs_encode(symbol), '{}_{}.csv'.format(SUBREDDIT, 'comment'))
	comments_data = pd.read_csv(comments_filename, sep='|')

	twitter_filename = os.path.join('..', TWITTER_DIR, fs_encode(symbol), 'tweets.csv')
	twitter_data = pd.read_csv(twitter_filename, sep=',')

	def combine_title_selftext(texts):
		title_and_text = []
		for t in texts:
			text = t.strip().split('.')
			new_text = ''
			for sentence in text:
				stripped = sentence.strip()
				if not stripped.isspace():
					new_text += stripped + '. '
			if not new_text.isspace():
				title_and_text.append(new_text)
		return ''.join(title_and_text)
	# Aggregate text
	submissions_data['text'] = submissions_data[['title', 'selftext']].astype(str).agg(combine_title_selftext, axis=1)
	comments_data['text'] = comments_data['body'].astype(str)
	twitter_data['text'] = twitter_data['tweet'].astype(str)

	# Convert into common date format
	submissions_data['date'] = [datetime.fromtimestamp(x) for x in submissions_data['created_utc']]
	comments_data['date'] = [datetime.fromtimestamp(x) for x in comments_data['created_utc']]
	twitter_data['date'] = pd.to_datetime(twitter_data['date'] + ' ' + twitter_data['time'], format='%Y-%m-%d %H:%M:%S') + timedelta(hours=8)

	# Assign score for tweets
	twitter_data['score'] = twitter_data['likes_count'] + twitter_data['retweets_count']

	# Assign replies
	submissions_data['replies'] = submissions_data['num_comments']
	comments_data['replies'] = 'NaN'
	twitter_data['replies'] = twitter_data['replies_count']

	# Set type
	submissions_data['type'] = '{} submission'.format(SUBREDDIT)
	comments_data['type'] = '{} comment'.format(SUBREDDIT)
	twitter_data['type'] = 'twitter'

	return [
		submissions_data,
		comments_data,
		twitter_data,
	]

def sample_annotate(symbol):
	split_data = get_social_data(symbol)

	social_data = pd.concat([
		split_data[0][['date', 'score', 'replies', 'text', 'type']],
		split_data[1][['date', 'score', 'replies', 'text', 'type']],
		split_data[2][['date', 'score', 'replies', 'text', 'type']],
	], axis=0)
	print(social_data)

	# Show data proportions
	num_total = len(social_data)
	print('Total data count: {}'.format(num_total))
	print('{} submissions: {}/{} = {:.2f} %'.format(SUBREDDIT, len(split_data[0]), num_total, len(split_data[0])*100/num_total))
	print('{} comments: {}/{} = {:.2f} %'.format(SUBREDDIT, len(split_data[1]), num_total, len(split_data[1])*100/num_total))
	print('tweets: {}/{} = {:.2f} %'.format(len(split_data[2]), num_total, len(split_data[2])*100/num_total))

	# Random sample
	rand = np.random.randint(len(social_data), size=5)
	sample = social_data.iloc[rand]

	# Annotate and print
	for _, line in sample.iterrows():
		blob = TextBlob(line['text'])
		print()
		print('type: {}'.format(line['type']))
		print('date: {}'.format(line['date']))
		print('score: {}'.format(line['score']))
		print('replies: {}'.format(line['replies']))
		print('text: {}'.format(line['text']))
		print('polarity: {}'.format(blob.sentiment.polarity))
		print('subjectivity: {}'.format(blob.sentiment.subjectivity))
		# print('tags: {}'.format(blob.tags))
		# print('noun phrases: {}'.format(blob.noun_phrases))
		# for sentence in blob.sentences:
		# 	print(sentence)
		# 	print(sentence.sentiment)
		print()

def sample_show(symbol):
	class NpEncoder(json.JSONEncoder):
		"""Converts objects to JSON-serializable types for printing.
		"""
		def default(self, obj):
			if isinstance(obj, np.integer):
				return int(obj)
			elif isinstance(obj, np.floating):
				return float(obj)
			elif isinstance(obj, np.bool_):
				return bool(obj)
			elif isinstance(obj, np.ndarray):
				return obj.tolist()
			elif isinstance(obj, pd.Timestamp):
				return str(obj)
			else:
				return super(NpEncoder, self).default(obj)

	data = get_social_data(symbol)
	print('Reddit submission example', json.dumps(data[0].iloc[0].to_dict(), indent=4, sort_keys=True, cls=NpEncoder))
	print('Reddit comment example', json.dumps(data[1].iloc[0].to_dict(), indent=4, sort_keys=True, cls=NpEncoder))
	print('Tweet example', json.dumps(data[2].iloc[0].to_dict(), indent=4, sort_keys=True, cls=NpEncoder))

if __name__ == '__main__':
	if len(sys.argv) == 2:
		symbol = sys.argv[1]
		# sample_show(symbol)
		sample_annotate(symbol)
	else:
		text = ' '.join(sys.argv[1:])
		print('\n', text)
		blob = TextBlob(text)
		print('polarity: {}'.format(blob.sentiment.polarity))
		print('subjectivity: {}'.format(blob.sentiment.subjectivity))
