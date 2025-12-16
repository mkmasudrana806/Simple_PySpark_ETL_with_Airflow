
-- ============== create top_products gold table ===============
create table if not exists top_products (
    product_id text,
    product_name text,
    category text,
    rating real,
    rating_count integer,
    discounted_price real,
    rank integer not null
);

