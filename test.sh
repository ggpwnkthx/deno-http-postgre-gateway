curl -sS -X POST 'http://localhost:8000/batch' \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer dbpass' \
    -H 'X-DB-User: dbuser' \
    --data-raw '{
        "statements": [
            { "sql": "create temporary table if not exists demo(id int, name text)" },
            { "sql": "insert into demo(id, name) values ($1, $2), ($3, $4)", "params": [1, "one", 2, "two"] },
            { "sql": "select * from demo order by id" }
        ]
    }'