# social_media_stock_data
Scrape historical Subreddit posts and Tweets related to stock symbols and company names.

Dependencies
```
pip install pandas==1.2.2
pip install requests==2.25.1

git clone --depth=1 https://github.com/twintproject/twint.git
cd twint
pip3 install . -r requirements.txt
```

Run to write/update `reddit_data` and `twitter_data`
```
python scrape_social.py
```
