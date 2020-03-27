# Running tests towards local database

## Get postgres docker image
`docker pull postgres`

## Run postgres image and forward port to localhost
`docker run --name postgres -e POSTGRES_PASSWORD=your_password -p 5432:5432 postgres`

## Connect to postgres database and create events table
`docker run -it --rm --link postgres:postgres postgres psql -h postgres -U postgres`

Paste contents of `schema.sql` to create table.

## Run tests
`env POSTGRES_CONN_STRING="user=postgres password=your_password sslmode=disable" go test -v`
