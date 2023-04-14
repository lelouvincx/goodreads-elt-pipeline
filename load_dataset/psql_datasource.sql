CREATE DATABASE IF NOT EXISTS goodreads;
\c goodreads

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS recommendations;

CREATE TABLE IF NOT EXISTS gold.genre (
  Id SERIAL PRIMARY KEY,
  Name VARCHAR(63) UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.book_genre (
  BookISBN VARCHAR(31) NOT NULL,
  GenreId INT NOT NULL,
  PRIMARY KEY (BookISBN, GenreId)
);

CREATE TABLE IF NOT EXISTS gold.book_with_info (
  ISBN VARCHAR(31) PRIMARY KEY,
  Name VARCHAR(31),
  Authors VARCHAR(31),
  Language VARCHAR(7),
  Description TEXT,
  PagesNumber INT
);

CREATE TABLE IF NOT EXISTS gold.book_with_publish (
  ISBN VARCHAR(31) PRIMARY KEY,
  Publisher VARCHAR(31),
  PublishYear VARCHAR(31),
  PublishMonth INT,
  PublishDay INT
);

CREATE TABLE IF NOT EXISTS gold.book_with_rating (
  ISBN VARCHAR(31) PRIMARY KEY,
  Rating FLOAT,
  RatingDist5 INT,
  RatingDist4 INT,
  RatingDist3 INT,
  RatingDist2 INT,
  RatingDist1 INT,
  RatingDistTotal INT,
  CountOfTextReviews INT
);
