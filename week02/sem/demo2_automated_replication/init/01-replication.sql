CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
SELECT pg_create_physical_replication_slot('replication_slot_1');

/*
    немного данных, чтобы было что смотреть
*/
CREATE TABLE orders(
    id         serial PRIMARY KEY,
    amount     numeric,
    created_at timestamptz DEFAULT now()
);
INSERT INTO orders(amount) SELECT random() * 100 FROM generate_series(1, 1000);
