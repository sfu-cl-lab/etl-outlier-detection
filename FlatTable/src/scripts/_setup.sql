


DROP SCHEMA IF EXISTS unielwin_convert; 
create schema unielwin_convert;


USE unielwin_convert;
SET storage_engine=INNODB;





CREATE TABLE a_CT AS SELECT MULT,
    `popularity(prof0)`,
    `teachingability(prof0)`,
    `intelligence(student0)`,
    `ranking(student0)`,
    `capability(prof0,student0)`,
    `salary(prof0,student0)`,
    a FROM unielwin_CT.a_CT;


