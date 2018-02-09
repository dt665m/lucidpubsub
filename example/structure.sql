CREATE DATABASE testdata;
GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;

\c testdata

CREATE TABLE checkpoint (
	id BIGINT primary key,
	checkpoint BIGINT not null
);

CREATE TABLE finaldata (
	id BIGINT primary key,
	total BIGINT not null
);

CREATE TABLE events (
	sequence BIGINT not null,
	data BYTEA not null
);