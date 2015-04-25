myinput = LOAD '/Users/Stefano/Documents/BigData/DataGenerator/data/esempio.txt' USING TextLoader() as (myword:chararray);
words = FOREACH myinput GENERATE FLATTEN(TOKENIZE(*)) as n;
wordsFiltered = filter words by (n matches '^[a-z].*');
grouped = GROUP wordsFiltered BY $0;
counts = FOREACH grouped GENERATE group, COUNT(wordsFiltered);
WordCountSorted = ORDER counts BY $1 DESC;
store WordCountSorted into '/Users/Stefano/Documents/pig-0.14.0/pigoutput' using PigStorage();
