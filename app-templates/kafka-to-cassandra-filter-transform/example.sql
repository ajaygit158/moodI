cqlsh> CREATE KEYSPACE testdb WITH  replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

cqlsh> DESCRIBE keyspaces;

cqlsh> USE testdb;

cqlsh:testdb> CREATE TABLE IF NOT EXISTS testdb . dt_meta (dt_app_id TEXT, dt_operator_id INT, dt_window BIGINT, PRIMARY KEY (dt_app_id, dt_operator_id));

cqlsh:testdb> CREATE TABLE IF NOT EXISTS testdb . test_event_output_table (accountNumber int PRIMARY KEY,name varchar,amount int);

