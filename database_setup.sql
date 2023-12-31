CREATE USER demo WITH PASSWORD 'user_password_here';
create role analyst;
create schema source_tables;
create schema pipeline;
grant usage on schema source_tables to analyst;
grant select on all tables in schema source_tables to analyst;
grant all privileges on schema pipeline to analyst;
GRANT CREATE ON DATABASE postgres TO analyst;
grant analyst to demo;
create schema scratch;
grant all privileges on schema scratch to analyst;
select * from pg_matviews;
SELECT * FROM pg_stat_activity;
