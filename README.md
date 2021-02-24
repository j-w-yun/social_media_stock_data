# social_media_stock_data
Scrape historical Subreddit posts and Tweets related to stock symbols and company names.

Dependencies
```
pip install pandas==1.2.2
pip install requests==2.25.1
pip install stem==1.8.0
...
```

Requires tor. Without it to bypass rate limits, downloading this dataset could take months.

Set SOCKS proxy on port 9050. Set controller on port 9051.
```
# Set controller password as environmental variable
export TOR_CONTROLLER_PW="your_password_here"
```

Set the Subreddit to scrape.
```
reddit = REDDIT(directory='reddit_data', subreddit='wallstreetbets')
```

Run to write/update `reddit_data`
```
python scrape_social.py -r
```

Run to write/update `twitter_data`
```
python scrape_social.py -t
```

Run to write/update both `reddit_data` and `twitter_data`
```
python scrape_social.py -a
```
