-- Full load reference database
DROP DATABASE IF EXISTS goodreads;
CREATE DATABASE goodreads;
USE goodreads;

-- Load books
DROP TABLE IF EXISTS goodreads.book;
CREATE TABLE goodreads.book (
  Id INT NOT NULL AUTO_INCREMENT,
  Name VARCHAR(255),
  Authors VARCHAR(255),
  ISBN VARCHAR(31),
  Rating DOUBLE PRECISION,
  PublishYear VARCHAR(31),
  PublishMonth INT,
  PublishDay INT,
  Publisher VARCHAR(255),
  RatingDist5 VARCHAR(31),
  RatingDist4 VARCHAR(31),
  RatingDist3 VARCHAR(31),
  RatingDist2 VARCHAR(31),
  RatingDist1 VARCHAR(31),
  RatingDistTotal VARCHAR(31),
  CountsOfReview INT,
  Language VARCHAR(7),
  Description TEXT,
  `Count of text reviews` INT,
  PagesNumber INT,
  PRIMARY KEY (Id)
);

-- Load genre
DROP TABLE IF EXISTS goodreads.genre;
CREATE TABLE goodreads.genre (
  Id INT NOT NULL AUTO_INCREMENT,
  Name VARCHAR(255) UNIQUE,
  PRIMARY KEY (Id)
);

-- Load book_genre
DROP TABLE IF EXISTS goodreads.book_genre;
CREATE TABLE goodreads.book_genre (
  BookISBN VARCHAR(31) NOT NULL,
  GenreId INT NOT NULL,
  PRIMARY KEY (BookISBN, GenreId)
);

-- Load book_download_link
DROP TABLE IF EXISTS goodreads.book_download_link;
CREATE TABLE goodreads.book_download_link (
  BookISBN VARCHAR(31) NOT NULL UNIQUE,
  Link VARCHAR(255) NOT NULL,
  PRIMARY KEY (BookISBN, Link)
);
