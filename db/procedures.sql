-- Run this against the `blogs` schema (MySQL 8+)
USE blogs;

DROP PROCEDURE IF EXISTS sp_bulk_delete_blogs;
DROP PROCEDURE IF EXISTS sp_bulk_update_blogs;
DROP PROCEDURE IF EXISTS sp_update_blog_content;
DROP PROCEDURE IF EXISTS sp_delete_blog;
DROP PROCEDURE IF EXISTS sp_create_blog;

DELIMITER //
CREATE PROCEDURE sp_bulk_delete_blogs(IN p_ids_json JSON)
BEGIN
  DELETE b
  FROM blogs b
  JOIN JSON_TABLE(p_ids_json, '$[*]' COLUMNS(id BIGINT PATH '$')) jt
    ON b.id = jt.id;
  SELECT ROW_COUNT() AS deleted;
END //

CREATE PROCEDURE sp_bulk_update_blogs(
  IN p_ids_json JSON,
  IN p_genre VARCHAR(64),
  IN p_location VARCHAR(128),
  IN p_content MEDIUMTEXT
)
BEGIN
  UPDATE blogs b
  JOIN JSON_TABLE(p_ids_json, '$[*]' COLUMNS(id BIGINT PATH '$')) jt
    ON b.id = jt.id
  SET b.genre = COALESCE(NULLIF(p_genre, ''), b.genre),
      b.location = COALESCE(NULLIF(p_location, ''), b.location),
      b.content = COALESCE(NULLIF(p_content, ''), b.content),
      b.updated_at = NOW(6);
  SELECT ROW_COUNT() AS updated;
END //

CREATE PROCEDURE sp_update_blog_content(
  IN p_id BIGINT,
  IN p_content MEDIUMTEXT,
  IN p_updated_at DATETIME(6)
)
BEGIN
  UPDATE blogs SET content = p_content, updated_at = p_updated_at WHERE id = p_id;
  SELECT ROW_COUNT() AS updated;
END //

CREATE PROCEDURE sp_delete_blog(IN p_id BIGINT)
BEGIN
  DELETE FROM blogs WHERE id = p_id;
  SELECT ROW_COUNT() AS deleted;
END //

CREATE PROCEDURE sp_create_blog(
  IN p_client_msg_id CHAR(36),
  IN p_author VARCHAR(128),
  IN p_content MEDIUMTEXT,
  IN p_genre VARCHAR(64),
  IN p_location VARCHAR(128),
  IN p_created_at DATETIME(6)
)
BEGIN
  INSERT INTO blogs (client_msg_id, author, created_at, updated_at, genre, location, content)
  VALUES (p_client_msg_id, p_author, p_created_at, p_created_at, p_genre, p_location, p_content);
  SELECT LAST_INSERT_ID() AS id;
END //
DELIMITER ; 