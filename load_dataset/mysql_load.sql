LOAD DATA LOCAL INFILE '/tmp/dataset/book_full.csv'
INTO TABLE goodreads.book
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/tmp/dataset/my_book.csv'
-- INTO TABLE goodreads.book
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;
--
-- LOAD DATA LOCAL INFILE '/tmp/dataset/genre.csv'
-- INTO TABLE goodreads.genre
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;
--
-- LOAD DATA LOCAL INFILE '/tmp/dataset/book_genre.csv'
-- INTO TABLE goodreads.book_genre
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;
--
-- LOAD DATA LOCAL INFILE '/tmp/dataset/book_download_link.csv'
-- INTO TABLE goodreads.book_download_link
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;
