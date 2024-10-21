# https://hub.docker.com/_/postgres
docker run --name repro-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 --rm postgres