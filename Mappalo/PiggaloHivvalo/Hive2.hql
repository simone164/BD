CREATE TABLE docs(d string, p1 string, p2 string, p3 string, p4 string) row format delimited fields terminated by ',' stored as textfile;
load data local inpath '/Users/Stefano/Documents/BigData/DataGenerator/data/esempio.txt' into table docs;

CREATE TABLE onlyTrimestre AS
SELECT p1 as prodotto,substr(d,0,6) as data FROM docs WHERE p1 NOT LIKE '%NULL%' AND (d LIKE '%-0-%' OR d LIKE '%-1-%' OR d LIKE '%-2-%') UNION ALL
SELECT p2 as prodotto,substr(d,0,6) as data FROM docs WHERE p2 NOT LIKE '%NULL%' AND (d LIKE '%-0-%' OR d LIKE '%-1-%' OR d LIKE '%-2-%') UNION ALL
SELECT p3 as prodotto,substr(d,0,6) as data FROM docs WHERE p3 NOT LIKE '%NULL%' AND (d LIKE '%-0-%' OR d LIKE '%-1-%' OR d LIKE '%-2-%') UNION ALL
SELECT p4 as prodotto,substr(d,0,6) as data FROM docs WHERE p4 NOT LIKE '%NULL%' AND (d LIKE '%-0-%' OR d LIKE '%-1-%' OR d LIKE '%-2-%')
;

CREATE TABLE finaleHivvalo AS
SELECT prodotto, data, count(1) 
FROM onlyTrimestre
GROUP BY prodotto, data;