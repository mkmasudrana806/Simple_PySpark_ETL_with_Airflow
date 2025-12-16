-- ============== create products_summary gold table ===============
do $$
begin
	if not exists (select 1 from pg_type where typname = 'rating_bucket_enum')
	then create type rating_bucket_enum as enum (
		'Excellent',
		'Good',
		'Average',
		'Poor'
	);
	end if;
end$$;
 

do $$
begin
    if not exists (select 1 from pg_type where typname = 'rating_strength_enum') then
        create type rating_strength_enum as enum (
            'very high',
            'high',
            'medium',
            'low'
        );
    end if;
end$$;



create table if not exists product_summary (
    product_id text,
    product_title text,
    category text,
    avg_rating double precision,
    rating_count integer,
    discounted_price real,
    actual_price real,
    discount_percentage integer,
    price_difference real,
    rating_bucket rating_bucket_enum,
    rating_strength rating_strength_enum
);
