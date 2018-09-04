A Simple nodeJS server to pick up webhooks from Segment.com and insert into a Postgres database.

Database connection parameters can be provided as environment variables,  
PGUSER='xxx' PGHOST='yyy' PGPASSWORD='zzz' PGDATABASE='database' node server.js