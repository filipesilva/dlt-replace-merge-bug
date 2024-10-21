CREATE TABLE jobs (
    number INTEGER PRIMARY KEY NOT NULL,
    name TEXT
);

INSERT INTO jobs (number, name) 
VALUES (1, 'foo'), (2, 'bar');