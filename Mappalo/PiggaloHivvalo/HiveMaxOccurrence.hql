CREATE TABLE docs (line STRING);
LOAD DATA LOCAL INPATH '/Users/Stefano/Documents/BigData/DataGenerator/data/esempio.txt'
 OVERWRITE INTO TABLE docs;
CREATE TABLE word_counts AS
SELECT one.element, COUNT(1) AS count FROM 
(SELECT explode(split(line, ',')) AS element FROM docs)one WHERE NOT (one.element RLIKE "201.*")  
GROUP BY one.element
ORDER BY count DESC;
