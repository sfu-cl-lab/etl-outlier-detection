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


/* Simplly copy the a_CT table from @database@_CT  */

CREATE TABLE a_CT AS SELECT MULT,
    `popularity(prof0)`,
    `teachingability(prof0)`,
    `intelligence(student0)`,
    `ranking(student0)`,
    `capability(prof0,student0)`,
    `salary(prof0,student0)`,
    a FROM unielwin_CT.a_CT;





