

-- ============== create discount_effectiveness gold table ===============
do $$
begin
    if not exists (select 1 from pg_type where typname = 'discount_bucket_enum') then
        create type discount_bucket_enum as enum (
            'Very High Discount',
            'High Discount',
            'Medium Discount',
            'Low Discount'
        );
    end if;
end$$;


create table if not exists discount_effectiveness (
    discount_bucket discount_bucket_enum not null,
    avg_rating double precision,
    avg_rating_count double precision,
    total_products integer not null
);

