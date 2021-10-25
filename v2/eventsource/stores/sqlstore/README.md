# Running tests towards local database

To run the tests locally, you have to spin up a local postgres database, and set environment
variables to let the test connect to the database.

## Get postgres docker image
`docker pull postgres`

## Run postgres image and forward port to localhost
`docker run --name postgres -e POSTGRES_PASSWORD=your_password -p 5432:5432 postgres`

## Run tests
`env PGUSER="postgres" PGPASSWORD="your_password" PGSSLMODE="disable" go test -v`
