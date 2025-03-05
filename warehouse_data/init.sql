CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public.car_sales (
	sales_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	id_sales_nk int4 NULL,
	"year" int4 NULL,
	brand_car_id int4 NULL,
	transmission varchar NULL,
	id_state int4 NULL,
	"condition" float4 NULL,
	odometer float4 NULL,
	color varchar NULL,
	interior varchar NULL,
	mmr float4 NULL,
	selling_price float4 NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT car_sales_pk PRIMARY KEY (sales_id),
	CONSTRAINT car_sales_unique UNIQUE (id_sales_nk)
);