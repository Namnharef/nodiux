use nodiux;

/* drop database nodiux */
/* CREATE DATABASE nodiux CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; */

select * from searches;
select * from posts;


select * from searches;
select * from searches_cids;
select * from posts;
select * from mentions;
select * from post_emojis;
select * from hashtags;

SELECT posts.cid
        FROM posts
        INNER JOIN searches ON posts.cid = searches.cid
        WHERE searches.session_id = '776dd1ad-9bc6-4100-9675-9abdac5dcad7' limit 0, 10000000;
        
        
select * from searches where session_id='776dd1ad-9bc6-4100-9675-9abdac5dcad7'  limit 0, 10000000;

SHOW CREATE TABLE searches;

select * from searches_cids where session_id='356b522e-8650-4eaf-9873-a23d1b139aa4'
	and search_id='35ac314d-7030-497f-ae7a-754923fea501';
    
    
select max(bluesky_handle) bluesky_handle, searches.session_id, max(mode), max(query), max(resultlimit), max(ip_address), max(timestamp), searches.search_id, count(searches_cids.cid) as results
from searches 
left outer join searches_cids on searches.session_id=searches_cids.session_id and searches.search_id=searches_cids.search_id
where searches.bluesky_handle='nodiux.bsky.social'
group by searches.session_id, searches.search_id;

    WITH filtered_searches_cids AS (
        SELECT s.*, sc.cid
        FROM searches s
        LEFT OUTER JOIN searches_cids sc ON s.session_id = sc.session_id AND s.search_id = sc.search_id
        WHERE bluesky_handle = 'nodiux.bsky.social'
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
    ORDER BY p.timestamp DESC ;

select * from searches;
select * from users;

CREATE TABLE user_async_searches (
    username VARCHAR(64) PRIMARY KEY, -- chiave unica per utente
    search_id VARCHAR(64) NOT NULL,
    progress INT DEFAULT 0,
    status ENUM('idle', 'running', 'completed', 'error') DEFAULT 'idle',
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


select * from searches where search_id='cba17418-b022-4d56-b66f-8273b0f509e5';
select * from searches_cids where search_id='cba17418-b022-4d56-b66f-8273b0f509e5' order by cid;
select * from posts where cid='bafyreiej7kplfyvf4q65ftjvbi33vsbfollnez67ufulwswdny7quzk7v4';
select * from searches_cids where cid=' bafyreierl6jg3jmpp6m26bnnbvh7kietgaycqihwhdgs6fvc2yzcqp7avq';

/*
searches_cids, CREATE TABLE `searches_cids` (
  `session_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `search_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `cid` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`session_id`,`search_id`,`cid`),
  KEY `cid` (`cid`),
  CONSTRAINT `searches_cids_ibfk_1` FOREIGN KEY (`cid`) REFERENCES `posts` (`cid`),
  CONSTRAINT `searches_cids_ibfk_2` FOREIGN KEY (`session_id`, `search_id`) REFERENCES `searches` (`session_id`, `search_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
*/

select * from user_async_searches;

/* delete from user_async_searches; */