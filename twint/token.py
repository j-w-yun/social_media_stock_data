import logging as logme
import os
import re
import requests
import time

from stem import Signal
from stem.control import Controller
from stem.util.log import get_logger
logger = get_logger()
logger.propagate = False


def get_tor_session():
	session = requests.session()
	session.proxies = {
		'http': 'socks5://127.0.0.1:9050',
		'https': 'socks5://127.0.0.1:9050',
	}
	return session


def renew_connection():
	with Controller.from_port(port=9051) as c:
		password = os.environ.get('TOR_CONTROLLER_PW')
		c.authenticate(password=password)
		c.signal(Signal.NEWNYM)


class TokenExpiryException(Exception):
	def __init__(self, msg):
		super().__init__(msg)


class RefreshTokenException(Exception):
	def __init__(self, msg):
		super().__init__(msg)


class Token:
	def __init__(self, config):
		self.renew()
		self.config = config
		self._retries = 100
		self._timeout = 100
		self.url = 'https://twitter.com'

	def renew(self):
		renew_connection()
		self._session = get_tor_session()
		self._session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0'})

	def _request(self):
		for attempt in range(self._retries + 1):
			# The request is newly prepared on each retry because of potential cookie updates.
			req = self._session.prepare_request(requests.Request('GET', self.url))
			logme.debug(f'Retrieving {req.url}')
			try:
				r = self._session.send(req, allow_redirects=True, timeout=self._timeout)
			except requests.exceptions.RequestException as exc:
				if attempt < self._retries:
					retrying = ', retrying'
					level = logme.WARNING
				else:
					retrying = ''
					level = logme.ERROR
				logme.log(level, f'Error retrieving {req.url}: {exc!r}{retrying}')
			else:
				success, msg = (True, None)
				msg = f': {msg}' if msg else ''

				if success:
					logme.debug(f'{req.url} retrieved successfully{msg}')
					return r
			if attempt < self._retries:
				# TODO : might wanna tweak this back-off timer
				sleep_time = 2.0 * 2 ** attempt
				logme.info(f'Waiting {sleep_time:.0f} seconds')
				time.sleep(sleep_time)
		else:
			msg = f'{self._retries + 1} requests to {self.url} failed, giving up.'
			logme.fatal(msg)
			self.config.Guest_token = None
			raise RefreshTokenException(msg)

	def refresh(self):
		logme.debug('Retrieving guest token')
		res = self._request()
		match = re.search(r'\("gt=(\d+);', res.text)

		while not match:
			self.renew()
			time.sleep(10)
			res = self._request()
			match = re.search(r'\("gt=(\d+);', res.text)

		if match:
			logme.debug('Found guest token in HTML')
			self.config.Guest_token = str(match.group(1))
		else:
			self.config.Guest_token = None
			raise RefreshTokenException('Could not find the Guest token in HTML')

