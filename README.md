# SALARY-PER-HOUR

## setup DB
* get docker image DB postgres
```
docker pull postgres:12.3-alpine
```

* docker run DB postgres
```
docker run --name db-postgres \
    -p 3000:5432 \
    -e POSTGRES_USER=admin \
    -e POSTGRES_PASSWORD=admin \
    -v vol-db-postgres:/var/lib/postgresql/data \
    -d postgres:12.3-alpine
```

* connect to DB via docker
```
psql -h localhost -U admin
```