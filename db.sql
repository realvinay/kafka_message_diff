create table messages (id serial NOT NULL PRIMARY KEY, message_key VARCHAR(50) NOT NULL UNIQUE, message JSON NOT NULL);