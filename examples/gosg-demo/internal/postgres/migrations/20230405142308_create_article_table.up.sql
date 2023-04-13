BEGIN;

CREATE TABLE IF NOT EXISTS articles (
	article_id serial PRIMARY KEY,
	title VARCHAR ( 150 ) NOT NULL,
	content TEXT NOT NULL,
	created_time TIMESTAMP NOT NULL
);

END;