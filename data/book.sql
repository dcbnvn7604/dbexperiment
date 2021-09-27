drop table if exists books;

create table books (
    id integer primary key,
    uuid uuid not null unique,
    name varchar(100) not null,
    description text not null,
    price integer not null,
    print_length smallint not null,
    file_size integer not null,
    publication_date date not null
);