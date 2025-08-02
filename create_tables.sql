        CREATE TABLE IF NOT EXISTS posts (
            cid VARCHAR(255) PRIMARY KEY,
            uri TEXT,
            author_did TEXT,
            author_handle TEXT,
            author_display_name TEXT,
            created_at DATETIME,
            text TEXT
        )

        CREATE TABLE IF NOT EXISTS mentions (
            cid VARCHAR(255),
            handle VARCHAR (255),
            PRIMARY KEY (cid, handle),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        )

        CREATE TABLE IF NOT EXISTS hashtags (
            cid VARCHAR(255),
            hashtag VARCHAR(255),
            PRIMARY KEY (cid, hashtag),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        )

        CREATE TABLE IF NOT EXISTS post_emojis (
            cid VARCHAR(255),
            emoji VARCHAR(100),
            PRIMARY KEY (cid, emoji),
            FOREIGN KEY (cid) REFERENCES posts(cid)
        )

        CREATE TABLE IF NOT EXISTS searches (
            bluesky_handle TEXT,
            session_id VARCHAR(255),
            mode VARCHAR(80),
            query TEXT,
            resultlimit INT,
            ip_address VARCHAR(45),
            timestamp DATETIME,
            search_id VARCHAR(255),
            PRIMARY KEY (session_id, search_id)
        )
    
        CREATE TABLE IF NOT EXISTS searches_cids (
            session_id VARCHAR(255),
            search_id VARCHAR(255),
            cid VARCHAR(255),
            PRIMARY KEY (session_id, search_id, cid),
            FOREIGN KEY (cid) REFERENCES posts(cid),
            FOREIGN KEY (session_id, search_id) REFERENCES searches(session_id, search_id)
        )
