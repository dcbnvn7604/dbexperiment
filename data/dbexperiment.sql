drop table if exists entry_rel;
drop table if exists entry_child;
drop table if exists entry;

create table entry (
    id integer primary key,
    uuid uuid not null unique,
    text_field text not null,
    int_field integer not null,
    date_field date not null,
    point_field geometry not null,
    lat numeric,
    long numeric
);

create index date_field on entry (date_field);

create index text_field on entry using gin (to_tsvector('english', text_field));

create index point_field on entry using gist (point_field);

create table entry_child (
    uuid uuid not null unique,
    parrent_uuid uuid not null,
    date_field date not null,
    constraint fk_entry foreign key(parrent_uuid) references entry(uuid)
);

create index entry_child_date_field on entry_child (date_field);

create table entry_rel (
    uuid1 uuid not null,
    uuid2 uuid not null,
    constraint fk_entry_rel_uuid1 foreign key(uuid1) references entry(uuid),
    constraint fk_entry_rel_uuid2 foreign key(uuid2) references entry(uuid)
);

create index entry_rel_uuid1 on entry_rel (uuid1);
create index entry_rel_uuid2 on entry_rel (uuid2);

copy entry(id, uuid, text_field, int_field, date_field, point_field, lat, long)
from '/data/entry.csv'
delimiter ','
csv header;

copy entry_child(uuid, parrent_uuid, date_field)
from '/data/entry_child.csv'
delimiter ','
csv header;

copy entry_rel(uuid1, uuid2)
from '/data/entry_rel.csv'
delimiter ','
csv header;

copy entry_rel(uuid2, uuid1)
from '/data/entry_rel.csv'
delimiter ','
csv header;