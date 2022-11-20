-- Load input file from HDFS
inputFile = LOAD 'hdfs:///Ishita/episodeV_dialouges.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group, COUNT(words);
--DESC command
dialogue = ORDER cntd BY words DESC;
-- Store the result in HDFS
STORE dialogue INTO 'hdfs:///Ishita/results_V';