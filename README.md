# tg_parser

This project was created for async searching themes with telegram requests and collecting telegram groups and text posts (in common) from this groups,
with pushing this information to posgres database.

So, first you should create postgres database (in my situation it is 'tg_parser') with 3 tables (tg_groups, tg_photos, tg_msgs) like this:

CREATE TABLE tg_groups (
group_id bigserial not null unique PRIMARY KEY,
title text,
photo bigserial,
date timestamp,
verified bool,
megagroup bool,
signatures bool,
scam bool,
fake bool,
gigagroup bool,
join_to_send bool,
join_request bool,
forum bool,
access_hash int8,
participants_count serial4
);

CREATE TABLE tg_photos (
photo_id bigserial not null unique PRIMARY KEY,
dc_id smallserial,
has_video bool,
stripped_thumb bytea,
rfile_reference bytea
);

CREATE TABLE tg_msgs (
id bigserial not null unique PRIMARY KEY,
chat_id bigserial,
message text,
msg_text text,
raw_text text,
photo bigserial,
media text,
msg_views serial4,
forwards serial4,
replies serial4,
sender_id bigserial,
sender text
);

Also you can create special user for your parsers connection with this db.

then prepare next files:

1) db_config.ini - settings for connection to your db
[database]
db_type = your_db_type (in my case it postgresql)
db_user = your_db_user
db_pass = your_db_password
db_host = your_db_host (for example localhost)
db_port = your_port (as a usually 5432)
db_name = your_database_name

2) tg_config.ini - settings for connection to your telegram account
[Telegram]
api_id = your_api_id
api_hash = your_api_hash
api_title = 'your_api_title'
phone = your_phone
username = your_username

3) themes - this file includes all your bag of words or phrases for your search which separated by rows (1 row - 1 request), for example:
python 
it news
job in digital
