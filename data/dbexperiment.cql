match (e) detach delete e;

drop constraint entry_uuid if exists;
create constraint entry_uuid if not exists on (e:Entry) assert e.uuid is unique;

load csv with headers from 'file:///entry.csv' as row
merge (e:Entry {uuid: row.uuid});

load csv with headers from 'file:///entry_rel.csv' as row
match (e1:Entry), (e2:Entry)
where e1.uuid = row.uuid1 and e2.uuid = row.uuid2
merge (e1)-[:rel]->(e2)