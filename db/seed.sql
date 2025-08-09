-- MySQL 8 seed script (100 authors × 10 genres × 10 locations = 10,000 rows)
-- Run in DataGrip against schema `blogs`. Uncomment TRUNCATE if you want a clean slate.

-- Optional: wipe existing data
-- TRUNCATE TABLE blogs;

USE blogs;

-- Base time
SET @now := NOW(6);

INSERT INTO blogs (
  client_msg_id, author, created_at, updated_at, genre, location, content
)
WITH RECURSIVE
a(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM a WHERE n < 100),
g(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM g WHERE n < 10),
l(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM l WHERE n < 10),
authors AS (
  SELECT CONCAT('author_', LPAD(n, 3, '0')) AS author FROM a
),
genres AS (
  SELECT CONCAT('genre_', LPAD(n, 2, '0')) AS genre FROM g
),
locations AS (
  SELECT CONCAT('location_', LPAD(n, 2, '0')) AS location FROM l
)
SELECT
  s.client_msg_id,
  s.author,
  s.created_at,
  LEAST(@now, TIMESTAMPADD(SECOND, FLOOR(RAND()*86400), s.created_at)) AS updated_at,
  s.genre,
  s.location,
  s.content
FROM (
  SELECT
    UUID() AS client_msg_id,
    a.author,
    TIMESTAMPADD(SECOND, -FLOOR(RAND()*2592000), @now) AS created_at,
    g.genre,
    l.location,
    CONCAT(
      'Sample blog by ', a.author,
      ' in ', g.genre,
      ' at ', l.location,
      ' — content generated for testing.'
    ) AS content
  FROM authors a
  CROSS JOIN genres g
  CROSS JOIN locations l
) AS s; 