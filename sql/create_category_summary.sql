
-- ============== create category_summary gold table ===============

create table if not exists category_summary (
    category text,
    total_products integer not null,
    avg_rating real,
    avg_rating_count real,
    avg_discount_percentage real,
    avg_price_difference real
);
