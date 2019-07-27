import html
import logging
import math
import re
from datetime import datetime
from threading import Event
from urllib.parse import urlparse

import tweepy
from telegram.error import TelegramError
from telegram.ext import Job

from models import TwitterUser, Tweet, Subscription, TelegramChat

INFO_CLEANUP = {
	'NOTFOUND': 'Your subscription to @{} was removed because that profile doesn\'t exist anymore. Maybe the account\'s name changed?',
	'PROTECTED': 'Your subscription to @{} was removed because that profile is protected and can\'t be fetched.',
}

class FetchAndSendTweetsJob(Job):
	# Twitter API rate limit parameters
	LIMIT_WINDOW = 15 * 60
	LIMIT_COUNT = 300
	MIN_INTERVAL = 60
	TWEET_BATCH_INSERT_COUNT = 100

	@property
	def interval(self):
		tw_count = (TwitterUser.select()
					.join(Subscription)
					.group_by(TwitterUser)
					.count())
		if tw_count >= self.LIMIT_COUNT:
			return self.LIMIT_WINDOW
		res = math.ceil(tw_count * self.LIMIT_WINDOW / self.LIMIT_COUNT)
		return max(self.MIN_INTERVAL, res)

	def __init__(self, context=None):
		self.days = tuple(range(7))
		self.repeat = True
		self.context = context
		self.name = self.__class__.__name__
		self._remove = Event()
		self._enabled = Event()
		self._enabled.set()
		self.logger = logging.getLogger(self.name)

	def run(self, bot):
		self.logger.debug('Fetching tweets...')
		tweet_rows = []
		# fetch the tw users' tweets
		tw_users = list((TwitterUser.select()
						.join(Subscription)
						.group_by(TwitterUser)
						.order_by(TwitterUser.last_fetched)))
		updated_tw_users = []
		users_to_cleanup = []

		for tw_user in tw_users:
			try:
				if tw_user.last_tweet_id == 0:
					# get just the latest tweet
					self.logger.debug(
						'Fetching latest tweet by {}'.format(tw_user.screen_name))
					tweets = bot.tw.user_timeline(
						screen_name=tw_user.screen_name,
						count=1,
						tweet_mode='extended')
				else:
					# get the fresh tweets
					self.logger.debug(
						'Fetching new tweets from {}'.format(tw_user.screen_name))
					tweets = bot.tw.user_timeline(
						screen_name=tw_user.screen_name,
						since_id=tw_user.last_tweet_id,
						tweet_mode='extended')
				updated_tw_users.append(tw_user)
			except tweepy.error.TweepError as e:
				sc = e.response.status_code
				if sc == 429:
					self.logger.debug('- Hit ratelimit, breaking.')
					break

				if sc == 401:
					users_to_cleanup.append((tw_user, 'PROTECTED'))
					self.logger.debug('- Protected tweets here. Cleaning up this user')
					continue

				if sc == 404:
					users_to_cleanup.append((tw_user, 'NOTFOUND'))
					self.logger.debug('- 404? Maybe screen name changed? Cleaning up this user')
					continue

				self.logger.debug(
					'- Unknown exception, Status code {}'.format(sc))
				continue

			for tweet in tweets:
				retweet = False
				if 'retweeted_status' in tweet._json:
					retweet = True

				# NOTE: tweet.full_text doesn't work for retweets,
				# see https://stackoverflow.com/a/48967803

				# use current tweet by default
				tweet_data = tweet
				if retweet:
					# use original tweet to text processing
					tweet_data = tweet.retweeted_status

				tw_text = tweet_data.full_text

				self.logger.debug('- Got tweet: {}'.format(tw_text))

				# Check if tweet contains media, else check if it contains a link to an image
				extensions = ('.jpg', '.jpeg', '.png', '.gif')
				pattern = '[(%s)]$' % ')('.join(extensions)
				photo_url = ''
				tweet_text = html.unescape(tw_text)
				if 'media' in tweet_data.entities:
					photo_url = tweet_data.entities['media'][0]['media_url_https']
				else:
					for url_entity in tweet_data.entities['urls']:
						expanded_url = url_entity['expanded_url']
						if re.search(pattern, expanded_url):
							photo_url = expanded_url
							break
				if photo_url:
					self.logger.debug('- - Found media URL in tweet: ' + photo_url)

				for url_entity in tweet_data.entities['urls']:
					expanded_url = url_entity['expanded_url']
					parsed_url = urlparse(expanded_url)

					indices = url_entity['indices']
					display_url = tw_text[indices[0]:indices[1]]

					replace_text = expanded_url

					if parsed_url.netloc == 'twitter.com':
						re_pattern = '/(?P<username>.+)/status/(?P<twit_id>[0-9]+)'
						re_result = re.match(re_pattern, parsed_url.path)
						if re_result != None:
							try:
								commented_tweet = bot.tw.get_status(re_result.group('twit_id'), tweet_mode='extended')
							except:
								pass
							else:
								# TODO: implement hack for retweets
								# TODO: move text formatting into send_tweet, store only basic tweet bits
								tweet_text = 'comment:\n' + tweet_text
								replace_text = '\n\noriginal tweet:\n«{}»'.format(commented_tweet.full_text)

					tweet_text = tweet_text.replace(display_url, replace_text)

				tw_data = {
					'tw_id': tweet.id,
					'text': tweet_text,
					'created_at': tweet.created_at,
					'twitter_user': tw_user,
					'photo_url': photo_url,
				}

				if retweet:
					# store original screen name
					tw_data['original_name'] = tweet_data.user.screen_name

				try:
					t = Tweet.get(Tweet.tw_id == tweet.id)
					self.logger.warning('Got duplicated tw_id on this tweet:')
					self.logger.warning(str(tw_data))
				except Tweet.DoesNotExist:
					tweet_rows.append(tw_data)

				if len(tweet_rows) >= self.TWEET_BATCH_INSERT_COUNT:
					Tweet.insert_many(tweet_rows).execute()
					tweet_rows = []

		TwitterUser.update(last_fetched=datetime.now()) \
			.where(TwitterUser.id << [tw.id for tw in updated_tw_users]).execute()

		if updated_tw_users and tweet_rows:
			Tweet.insert_many(tweet_rows).execute()

		# send last tweets to newcomers
		subscriptions = list(Subscription.select()
							.where(Subscription.last_tweet_id == 0))
		for s in subscriptions:
			self.logger.debug(
				'Checking new subscription {} {}'.format(s.tg_chat.chat_id, s.tw_user.screen_name))

			try:
				tw = s.tw_user.tweets.select() \
					.order_by(Tweet.tw_id.desc()) \
					.first()
				if tw is None:
					self.logger.warning('Something fishy is going on here...')
				else:
					bot.send_tweet(s.tg_chat, tw)
					# save the latest tweet sent on this subscription
					s.last_tweet_id = tw.tw_id
					s.save()
			except IndexError:
				self.logger.debug('- No tweets available yet on {}'.format(s.tw_user.screen_name))

		# send the new tweets to existing subscribers
		query = '''SELECT S.* FROM subscription S
		INNER JOIN twitteruser TU
		ON S.tw_user_id = TU.id
		WHERE S.last_tweet_id <
		(
			SELECT tw_id
			FROM tweet T
			WHERE T.twitter_user_id = TU.id
			ORDER BY T.tw_id DESC
			LIMIT 1
		)
'''
		subscriptions = list(Subscription.raw(query))
		for s in subscriptions:
			# are there new tweets? send em all!
			self.logger.debug(
				'Checking subscription {} {}'.format(s.tg_chat.chat_id, s.tw_user.screen_name))

			self.logger.debug('- Some fresh tweets here which was not sended yet!')

			last_sended_tweet_id = s.last_tweet_id

			for tw in (s.tw_user.tweets.select()
								.where(Tweet.tw_id > s.last_tweet_id)
								.order_by(Tweet.tw_id.asc())
					):
				if bot.send_tweet(s.tg_chat, tw):
					last_sended_tweet_id = tw.tw_id
				else:
					break

			# save the latest tweet sent on this subscription
			s.last_tweet_id = last_sended_tweet_id
			s.save()
			continue

			self.logger.debug('- No new tweets here.')


		self.logger.debug('Starting tw_user cleanup')
		if not users_to_cleanup:
			self.logger.debug('- Nothing to cleanup')
		else:
			for tw_user, reason in users_to_cleanup:
				self.logger.debug('- Cleaning up subs on user @{}, {}'.format(tw_user.screen_name, reason))
				message = INFO_CLEANUP[reason].format(tw_user.screen_name)
				subs = list(tw_user.subscriptions)
				for s in subs:
					chat = s.tg_chat
					if chat.delete_soon:
						self.logger.debug ('- - skipping because of delete_soon chatid={}'.format(chat_id))
						continue
					chat_id = chat.chat_id
					self.logger.debug ('- - bye on chatid={}'.format(chat_id))
					s.delete_instance()

					try:
						bot.sendMessage(chat_id=chat_id, text=message)
					except TelegramError as e:
						self.logger.info('Couldn\'t send unsubscription notice of {} to chat {}: {}'.format(
							tw_user.screen_name, chat_id, e.message
						))

						delet_this = None

						if e.message == 'Bad Request: group chat was migrated to a supergroup chat':
							delet_this = True

						if e.message == 'Unauthorized':
							delet_this = True

						if delet_this:
							self.logger.info('Marking chat for deletion')
							chat.delete_soon = True
							chat.save()

			self.logger.debug('- Cleaning up TwitterUser @{}'.format(tw_user.screen_name, reason))
			tw_user.delete_instance()

			self.logger.debug ('- Cleanup finished')

		self.logger.debug('Cleaning up TelegramChats marked for deletion')
		for chat in TelegramChat.select().where(TelegramChat.delete_soon == True):
			chat.delete_instance(recursive=True)
			self.logger.debug('Deleting chat {}'.format(chat.chat_id))
