# backend/bluesky_client.py
import pandas as pd
import re
import emoji
from atproto import Client, models


def extract_mentions(text):
    return re.findall(r'@([\w\.]+)', text or "")

def extract_hashtags(text):
    return re.findall(r'#(\w+)', text or "")

def normalize_handle(handle):
    return handle.lower().replace('.bsky.social', '')

def fetch_posts_old(username, password, mode, query, limit):
    try:
        client = Client()
        client.login(username, password)
    except Exception as e:
        return None, f"Login failed: {str(e)}"

    try:
        posts = []

        if mode == "Hashtag":
            params = models.AppBskyFeedSearchPosts.Params(q=f"#{query}", limit=limit)
            result = client.app.bsky.feed.search_posts(params)
            raw_posts = result.posts
        else:
            handle = query.strip()
            params = models.AppBskyFeedGetAuthorFeed.Params(actor=handle, limit=limit)
            result = client.app.bsky.feed.get_author_feed(params)
            raw_posts = [item.post for item in result.feed]

        for post in raw_posts:
            posts.append({
                'uri': post.uri,
                'cid': post.cid,
                'author_did': post.author.did,
                'author_handle': normalize_handle(post.author.handle),
                'author_display_name': post.author.display_name,
                'created_at': post.record.created_at,
                'text': post.record.text,
                'mentions': extract_mentions(post.record.text),
                'hashtags': extract_hashtags(post.record.text),
                'emojis': [c for c in post.record.text if c in emoji.EMOJI_DATA]
            })
            #print({
            #    'uri': post.uri,
            #    'cid': post.cid,
            #    'author_did': post.author.did,
            #    'author_handle': normalize_handle(post.author.handle),
            #    'author_display_name': post.author.display_name,
            #    'created_at': post.record.created_at,
            #    'text': post.record.text,
            #    'mentions': extract_mentions(post.record.text),
            #    'hashtags': extract_hashtags(post.record.text),
            #    'emojis': [c for c in post.record.text if c in emoji.EMOJI_DATA]
            #})
            #})
            
        df = pd.DataFrame(posts)
        df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601', errors='coerce', utc=True)
        return df, None

    except Exception as e:
        return None, f"Error fetching posts: {str(e)}"


def fetch_posts(username, password, mode, query, limit):
    try:
        client = Client()
        client.login(username, password)
    except Exception as e:
        return None, f"Login failed: {str(e)}"

    try:
        posts = []
        cursor = None

        while len(posts) < limit:
            fetch_limit = min(100, limit - len(posts))  # massimo 100 per chiamata

            if mode == "Hashtag":
                params = models.AppBskyFeedSearchPosts.Params(
                    q=f"#{query}", limit=fetch_limit, cursor=cursor
                )
                result = client.app.bsky.feed.search_posts(params)
                raw_posts = result.posts
                cursor = result.cursor
            else:
                handle = query.strip()
                params = models.AppBskyFeedGetAuthorFeed.Params(
                    actor=handle, limit=fetch_limit, cursor=cursor
                )
                result = client.app.bsky.feed.get_author_feed(params)
                raw_posts = [item.post for item in result.feed]
                cursor = result.cursor

            if not raw_posts:
                break  # Fine dei dati

            for post in raw_posts:
                posts.append({
                    'uri': post.uri,
                    'cid': post.cid,
                    'author_did': post.author.did,
                    'author_handle': normalize_handle(post.author.handle),
                    'author_display_name': post.author.display_name,
                    'created_at': post.record.created_at,
                    'text': post.record.text,
                    'mentions': extract_mentions(post.record.text),
                    'hashtags': extract_hashtags(post.record.text),
                    'emojis': [c for c in post.record.text if c in emoji.EMOJI_DATA]
                })

            if cursor is None:
                break  # Niente più pagine

        df = pd.DataFrame(posts)
        df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601', errors='coerce', utc=True)
        return df, None

    except Exception as e:
        return None, f"Error fetching posts: {str(e)}"
    


# Async version of fetch_posts
import time
def fetch_posts_async(username, password, mode, query, limit, update_progress_callback=None):
    print(f"fetch_posts_async: username={username}, mode={mode}, query={query}, limit={limit}")
    try:
        client = Client()
        client.login(username, password)
    except Exception as e:
        return None, f"Login failed: {str(e)}"

    try:
        posts = []
        cursor = None

        while len(posts) < limit:
            fetch_limit = min(100, limit - len(posts))

            if mode == "Hashtag":
                params = models.AppBskyFeedSearchPosts.Params(
                    q=f"#{query}", limit=fetch_limit, cursor=cursor
                )
                result = client.app.bsky.feed.search_posts(params)
                raw_posts = result.posts
                cursor = result.cursor
            else:
                handle = query.strip()
                params = models.AppBskyFeedGetAuthorFeed.Params(
                    actor=handle, limit=fetch_limit, cursor=cursor
                )
                result = client.app.bsky.feed.get_author_feed(params)
                raw_posts = [item.post for item in result.feed]
                cursor = result.cursor

            if not raw_posts:
                break

            for post in raw_posts:
                posts.append({
                    'uri': post.uri,
                    'cid': post.cid,
                    'author_did': post.author.did,
                    'author_handle': normalize_handle(post.author.handle),
                    'author_display_name': post.author.display_name,
                    'created_at': post.record.created_at,
                    'text': post.record.text,
                    'mentions': extract_mentions(post.record.text),
                    'hashtags': extract_hashtags(post.record.text),
                    'emojis': [c for c in post.record.text if c in emoji.EMOJI_DATA]
                })

            if update_progress_callback:
                update_progress_callback(len(posts), limit)

            if cursor is None:
                break

            time.sleep(0.4)  # throttling

        df = pd.DataFrame(posts)
        df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601', errors='coerce', utc=True)
        return df, None

    except Exception as e:
        return None, f"Error fetching posts: {str(e)}"



