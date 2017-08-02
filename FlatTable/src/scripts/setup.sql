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

CREATE TABLE ab_CT AS SELECT `ID(student0)`,
	MULT,
	`diff(course0)`,
    `popularity(prof0)`,
    `rating(course0)`,
    `teachingability(prof0)`,
    `intelligence(student0)`,
    `ranking(student0)`,
    `capability(prof0,student0)`,
    `salary(prof0,student0)`,
    `grade(course0,student0)`,
    `sat(course0,student0)`,
    a,
    b FROM unielwin_CT.`a,b_CT`;


CREATE TABLE test AS SELECT * FROM `ab_CT`; 



