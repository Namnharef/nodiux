import json
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime

# Patch temporanea per compatibilità con numpy 2.0
if not hasattr(np, 'float_'):
    np.float_ = np.float64

# SQL query to get searches
SQL_SEARCHES = '''
    WITH filtered_searches_cids AS (
        SELECT s.*, sc.cid
        FROM searches s
        LEFT OUTER JOIN searches_cids sc ON s.session_id = sc.session_id AND s.search_id = sc.search_id
        WHERE user = %s and (%s = '' OR s.search_id = %s)
    )
    SELECT p.*, h.hashtags, m.mentions, u.users, e.emojis
    FROM ( 
        SELECT fsc.bluesky_handle, fsc.session_id, fsc.mode, fsc.query, fsc.resultlimit, fsc.ip_address, fsc.timestamp, fsc.search_id, 
        count(fsc.cid) as posts
        FROM filtered_searches_cids fsc
        GROUP BY fsc.session_id, fsc.search_id 
    ) p
    LEFT OUTER JOIN (
        SELECT fsc.session_id, fsc.search_id, count(hashtags.hashtag) as hashtags
        FROM filtered_searches_cids fsc
        LEFT OUTER JOIN hashtags ON fsc.cid=hashtags.cid
        GROUP BY fsc.session_id, fsc.search_id
    ) h ON p.session_id=h.session_id AND p.search_id=h.search_id
    LEFT OUTER JOIN (
        SELECT fsc.session_id, fsc.search_id, count(mentions.handle) as mentions
        FROM filtered_searches_cids fsc
        LEFT OUTER JOIN mentions ON fsc.cid=mentions.cid
        GROUP BY fsc.session_id, fsc.search_id
    ) m ON p.session_id=m.session_id AND p.search_id=m.search_id   
    LEFT OUTER JOIN ( 
		SELECT ud.session_id, ud.search_id, count(ud.author_handle) as users
        FROM (
			SELECT DISTINCT fsc.session_id, fsc.search_id, p.author_handle
			FROM filtered_searches_cids fsc 
            LEFT OUTER JOIN posts p ON fsc.cid=p.cid
		) ud
        GROUP BY ud.session_id, ud.search_id 
    ) u ON p.session_id=u.session_id AND p.search_id=u.search_id   
    LEFT OUTER JOIN (
        SELECT fsc.session_id, fsc.search_id, count(pe.emoji) as emojis
        FROM filtered_searches_cids fsc
        LEFT OUTER JOIN post_emojis pe ON fsc.cid=pe.cid
        GROUP BY fsc.session_id, fsc.search_id
    ) e ON p.session_id=e.session_id AND p.search_id=e.search_id
    ORDER BY p.timestamp DESC 
'''


# Create MySQL tables
def mysql_create_tables(conn):
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            cid VARCHAR(255) PRIMARY KEY,
            uri TEXT,
            author_did TEXT,
            author_handle TEXT,
            author_display_name TEXT,
            created_at DATETIME,
            text TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mentions (
            cid VARCHAR(255),
            handle VARCHAR (255),
            PRIMARY KEY (cid, handle),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hashtags (
            cid VARCHAR(255),
            hashtag VARCHAR(255),
            PRIMARY KEY (cid, hashtag),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS post_emojis (
            cid VARCHAR(255),
            emoji VARCHAR(100),
            PRIMARY KEY (cid, emoji),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS searches (
            bluesky_handle TEXT,
            session_id VARCHAR(255),
            mode VARCHAR(80),
            query TEXT,
            resultlimit INT,
            ip_address VARCHAR(45),
            timestamp DATETIME,
            search_id VARCHAR(255),
            user VARCHAR(100),
            PRIMARY KEY (session_id, search_id)
        );
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS searches_cids (
            session_id VARCHAR(255),
            search_id VARCHAR(255),
            cid VARCHAR(255),
            PRIMARY KEY (session_id, search_id, cid),
            FOREIGN KEY (cid) REFERENCES posts(cid),
            FOREIGN KEY (session_id, search_id) REFERENCES searches(session_id, search_id)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username VARCHAR(255) PRIMARY KEY,
            password VARCHAR(255) NOT NULL
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_async_searches (
            username VARCHAR(64) PRIMARY KEY, -- chiave unica per utente
            search_id VARCHAR(64) NOT NULL,
            progress INT DEFAULT 0,
            status ENUM('idle', 'running', 'completed', 'error') DEFAULT 'idle',
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
    ''')

    return None
    

# Save posts to MySQL    
def save_to_mysql(df, bluesky_handle, session_id, conn, mode, query, limit, ip_address, timestamp, search_id, user):
    cursor = conn.cursor()

    # Insert data      
    cursor.execute('''
        INSERT IGNORE INTO searches
        (bluesky_handle, session_id, mode, query, resultlimit, ip_address, timestamp, search_id, user)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (bluesky_handle, session_id, mode, query, limit, ip_address, timestamp, search_id, user))
        
    if df is not None and not df.empty:
        for _, post in df.iterrows():
            cursor.execute('''
                INSERT IGNORE INTO posts (cid, uri, author_did, author_handle, created_at, text)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                post['cid'], post['uri'], post['author_did'], post['author_handle'],
                post['created_at'], post['text']
            ))

            for mention in post.get('mentions', []):
                cursor.execute('INSERT IGNORE INTO mentions (cid, handle) VALUES (%s, %s)', (post['cid'], mention))

            for hashtag in post.get('hashtags', []):
                cursor.execute('INSERT IGNORE INTO hashtags (cid, hashtag) VALUES (%s, %s)', (post['cid'], hashtag))

            for emoji in post.get('emojis', []):
                cursor.execute('INSERT IGNORE INTO post_emojis (cid, emoji) VALUES (%s, %s)', (post['cid'], emoji))               
        
            cursor.execute('''
                INSERT IGNORE INTO searches_cids
                (session_id, search_id, cid)
                VALUES (%s, %s, %s)
            ''', (session_id, search_id, post['cid']))

    conn.commit()
    cursor.close()
    return None

# Save posts to MySQL    
def add_to_mysql(df, session_id, conn, search_id):
    cursor = conn.cursor()

    print(f"add_to_mysql session_id={session_id}, search_id={search_id}")

    # Insert data                 
    if df is not None and not df.empty:
        for _, post in df.iterrows():
            cursor.execute('''
                INSERT INTO posts (cid, uri, author_did, author_handle, created_at, text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    text = VALUES(text)
            ''', (
                post['cid'], post['uri'], post['author_did'], post['author_handle'],
                post['created_at'], post['text']
            ))
            #print(f"Post {post['cid']} added/updated in MySQL")

            for mention in post.get('mentions', []):
                cursor.execute('INSERT IGNORE INTO mentions (cid, handle) VALUES (%s, %s)', (post['cid'], mention))

            for hashtag in post.get('hashtags', []):
                cursor.execute('INSERT IGNORE INTO hashtags (cid, hashtag) VALUES (%s, %s)', (post['cid'], hashtag))

            for emoji in post.get('emojis', []):
                cursor.execute('INSERT IGNORE INTO post_emojis (cid, emoji) VALUES (%s, %s)', (post['cid'], emoji))               
        
            #print(f"post cid {post['cid']}, session_id {session_id}, search_id {search_id}")
            cursor.execute('''
                INSERT IGNORE INTO searches_cids
                (session_id, search_id, cid)
                VALUES (%s, %s, %s)
            ''', (session_id, search_id, post['cid']))

    conn.commit()
    cursor.close()
    return None


# Load posts from MySQL
def load_from_mysql(session_id, conn, search_id):
    posts = []
    cursor = conn.cursor()

    # Prima seleziono i post base
    mySqlCmd = '''
        SELECT posts.cid, posts.uri, posts.author_did, posts.author_handle, posts.created_at, posts.text 
        FROM searches_cids
        INNER JOIN posts ON posts.cid = searches_cids.cid
        WHERE searches_cids.session_id = %s and searches_cids.search_id = %s
    '''
    print(f"{mySqlCmd}")
    cursor.execute(mySqlCmd, (session_id, search_id))
    posts_data = cursor.fetchall()
    print(f"{len(posts_data)} risultati trovati")
    for post_row in posts_data:
        post = {
            'cid': post_row[0],
            'uri': post_row[1],
            'author_did': post_row[2],
            'author_handle': post_row[3],
            'created_at': post_row[4],
            'text': post_row[5],
            'mentions': [],
            'hashtags': [],
            'emojis': []
        }
    
        # Mentions
        cursor.execute('SELECT handle FROM mentions WHERE cid = %s', (post['cid'],))
        mentions = cursor.fetchall()
        post['mentions'] = [m[0] for m in mentions]
    
        # Hashtags
        cursor.execute('SELECT hashtag FROM hashtags WHERE cid = %s', (post['cid'],))
        hashtags = cursor.fetchall()
        post['hashtags'] = [h[0] for h in hashtags]
    
        # Emojis
        cursor.execute('''
            SELECT pe.emoji 
            FROM post_emojis pe 
            WHERE pe.cid = %s
        ''', (post['cid'],))
        emojis = cursor.fetchall()
        post['emojis'] = [e[0] for e in emojis]                
    
        posts.append(post)
    
    cursor.close()

    df = pd.DataFrame(posts)
    
    return df


# Connect to MySQL
def mysql_connect(host, user, password, database):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database
    )
    return conn


# Get searches from MySQL
def mysql_get_searches(user, conn, search_id=None):
    searches = []
    cursor = conn.cursor()

    # Prima seleziono i post base
    mySqlCmd = SQL_SEARCHES
    # print(f"mysql_get_searches {mySqlCmd}")
    search_id = search_id if search_id else ''
    print(f"mysql_get_searches user={user}, search_id={search_id}")
    cursor.execute(SQL_SEARCHES, (user,search_id, search_id))
    searches_data = cursor.fetchall()
    print(f"mysql_get_searches {len(searches_data)} risultati trovati")
    # bluesky_handle, session_id, mode, query, resultlimit, ip_address, timestamp, search_id, posts, hashtags, mentions, users, emojis
    #'nodiux.bsky.social', '356b522e-8650-4eaf-9873-a23d1b139aa4', 'Hashtag', 'brexit', '222', '127.0.0.1', '2025-07-03 16:59:34', '566f0310-7e5c-47f2-abc8-0079dca83507', '222', '737', '40'
    for search_row in searches_data:
        search = {
            'bluesky_handle': search_row[0],
            'session_id': search_row[1],
            'mode': search_row[2],
            'query': search_row[3],
            'resultlimit': search_row[4],
            'ip_address': search_row[5],
            'timestamp': search_row[6],
            'search_id': search_row[7],
            'posts': search_row[8],
            'hashtags': search_row[9],
            'mentions': search_row[10],
            'users': search_row[11],
            'emojis': search_row[12]
        }

        searches.append(search)
    
    cursor.close()
    
    return searches


# Validate user credentials
def validate_user(username, password, conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s", (username, password))
    return cursor.fetchone()