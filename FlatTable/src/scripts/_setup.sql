


DROP SCHEMA IF EXISTS unielwin_convert; 
create schema unielwin_convert;


USE unielwin_convert;
SET storage_engine=INNODB;





DROP TABLE IF EXISTS `a,b_CT`;
CREATE TABLE `a,b_CT` AS SELECT * FROM unielwin_CT.`a,b_CT`;

DROP TABLE IF EXISTS `a_CT`;
CREATE TABLE `a_CT` AS SELECT * FROM unielwin_CT.`a_CT`;

DROP TABLE IF EXISTS `b_CT`;
CREATE TABLE `b_CT` AS SELECT * FROM unielwin_CT.`b_CT`;





