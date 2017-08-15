/****************************************************
Analyze schema information to prepare for statistical analysis.
@database@ stands for a generic database. This is replaced with the name of the actual target database schema by the program that calls this sql script.
*/

/*-- create schema @database@_convert;*/
DROP SCHEMA IF EXISTS @database@_convert; 
create schema @database@_convert;


USE @database@_convert;
SET storage_engine=INNODB;
/* allows adding foreign key constraints */


/* Simplly copy the CT tables from @database@_CT  */

DROP TABLE IF EXISTS `a,b_CT`;
CREATE TABLE `a,b_CT` AS SELECT * FROM unielwin_CT.`a,b_CT`;

DROP TABLE IF EXISTS `a_CT`;
CREATE TABLE `a_CT` AS SELECT * FROM unielwin_CT.`a_CT`;

DROP TABLE IF EXISTS `b_CT`;
CREATE TABLE `b_CT` AS SELECT * FROM unielwin_CT.`b_CT`;





