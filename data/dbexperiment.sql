drop table if exists entry_child;
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

create table entry_child (
    uuid uuid not null unique,
    parrent_uuid uuid not null,
    date_field date not null,
    constraint fk_entry foreign key(parrent_uuid) references entry(uuid)
);

create index entry_child_date_field on entry_child (date_field);

copy entry(id, uuid, text_field, int_field, date_field)
from '/data/entry.csv'
delimiter ','
csv header;

copy entry_child(uuid, parrent_uuid, date_field)
from '/data/entry_child.csv'
delimiter ','
csv header;