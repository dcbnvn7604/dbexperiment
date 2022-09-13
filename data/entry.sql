drop table if exists entry;

create table entry (
    id integer primary key,
    uuid uuid not null unique,
    text_field text not null,
    int_field integer not null,
    date_field date not null
);

create index date_field on entry (date_field);

create index text_field on entry using gin (to_tsvector('english', text_field));

copy entry(id, uuid, text_field, int_field, date_field)
from '/data/entry.csv'
delimiter ','
csv header;