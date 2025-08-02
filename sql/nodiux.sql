use nodiux;

/* drop database nodiux */
/* CREATE DATABASE nodiux CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; */

select * from searches;
select * from posts;

/*
alter table searches add column mode varchar(80);
alter table searches add column query text
alter table searches add column resultlimit int
alter table searches add column  ip_address VARCHAR(45)
alter table searches add  timestamp DATETIME
ALTER TABLE session_data_json MODIFY df_json LONGTEXT;
*/


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

/*
drop table  mentions ;
drop table  hashtags ;
drop table post_emojis ;
drop table searches_cids ;
drop table searches ;
drop table emojis ;
drop table  posts ;

delete from posts where cid<>'xx';
delete from mentions where cid<>'xx';
delete from hashtags where cid<>'xx';
delete from post_emojis where cid<>'xx';
delete from searches_cids where cid<>'xx';
delete from searches where cid<>'xx';
*/
select * from hashtags;

/*
# mentuions
ALTER TABLE mentions MODIFY COLUMN handle VARCHAR(255) NOT NULL;
delete from mentions where cid<>'xx';
ALTER TABLE mentions ADD PRIMARY KEY (handle, cid);

# hashtags
ALTER TABLE hashtags MODIFY COLUMN hashtag VARCHAR(255) NOT NULL;
delete from hashtags where cid<>'xx';
ALTER TABLE hashtags ADD PRIMARY KEY (hashtag, cid);

# post_emojis
ALTER TABLE post_emojis MODIFY COLUMN emoji VARCHAR(255) NOT NULL;
delete from post_emojis where cid<>'xx';
ALTER TABLE post_emojis ADD PRIMARY KEY (emoji, cid);

# searches
ALTER TABLE searches MODIFY COLUMN session_id VARCHAR(255) NOT NULL;
ALTER TABLE searches DROP COLUMN cid VARCHAR(255) NOT NULL;
delete from searches where cid<>'xx';
ALTER TABLE searches ADD PRIMARY KEY (session_id, cid);

alter table searches_cids ADD CONSTRAINT `searches_ibfk_2` FOREIGN KEY (`session_id`, `timestamp`) REFERENCES `searches` (`session_id`, `timestamp`)

alter table searches_cids DROP CONSTRAINT `searches_ibfk_2` 
*/

select * from searches_cids where session_id='356b522e-8650-4eaf-9873-a23d1b139aa4'
	and search_id='35ac314d-7030-497f-ae7a-754923fea501';
    
    
select max(bluesky_handle) bluesky_handle, searches.session_id, max(mode), max(query), max(resultlimit), max(ip_address), max(timestamp), searches.search_id, count(searches_cids.cid) as results
from searches 
left outer join searches_cids on searches.session_id=searches_cids.session_id and searches.search_id=searches_cids.search_id
where searches.bluesky_handle='nodiux.bsky.social'
group by searches.session_id, searches.search_id;

select * from searches;

WITH filtered_searches AS (
	SELECT * FROM searches WHERE bluesky_handle = 'nodiux.bsky.social'
)
SELECT 
	fs.bluesky_handle,
	fs.session_id,
	fs.mode,
	fs.query,
	fs.resultlimit,
	fs.ip_address,
	fs.timestamp,
	fs.search_id,
	sc.posts
FROM filtered_searches fs
LEFT JOIN (
	WITH filtered_cids AS (
		SELECT DISTINCT sc.cid
		FROM searches_cids sc
		INNER JOIN filtered_searches fs ON fs.session_id = sc.session_id AND fs.search_id = sc.search_id
    )
    SELECT *
    FROM hashtags h
    INNER JOIN fitered_cids fc 
	GROUP BY sc.session_id, sc.search_id
) sc ON fs.session_id = sc.session_id AND fs.search_id = sc.search_id  ;  

		SELECT sc.session_id, sc.search_id, COUNT(sc.cid) as posts
		FROM searches_cids sc
		INNER JOIN filtered_searches fs ON fs.session_id = sc.session_id AND fs.search_id = sc.search_id;

select * from hashtags;


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

ALTER TABLE searches ADD COLUMN user VARCHAR(100);
UPDATE searches SET user = 'nodiux' WHERE user IS NULL 

