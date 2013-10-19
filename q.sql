SET NAMES utf8;
SELECT COUNT(*) FROM `cmol_main_db`.`begining_match` WHERE `match_type` = 2 AND `match_begin_time` > UNIX_TIMESTAMP()
