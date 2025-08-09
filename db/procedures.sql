-- Run this against the `blogs` schema (MySQL 8+)
USE blogs;

DROP PROCEDURE IF EXISTS sp_bulk_delete_blogs;
DROP PROCEDURE IF EXISTS sp_bulk_update_blogs;

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
DELIMITER ; 