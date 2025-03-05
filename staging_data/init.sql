CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public.us_state (
	id_state serial4 NOT NULL,
	code varchar NULL,
	"name" varchar NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT us_state_pk PRIMARY KEY (id_state)
);

CREATE TABLE public.car_brand (
	brand_car_id int4 NOT NULL,
	brand_name varchar NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT car_brand_pk PRIMARY KEY (brand_car_id)
);

CREATE TABLE public.car_sales (
	id_sales serial4 NOT NULL,
	"year" varchar NULL,
	brand_car varchar NULL,
	transmission varchar NULL,
	state varchar NULL,
	"condition" varchar NULL,
	odometer varchar NULL,
	color varchar NULL,
	interior varchar NULL,
	mmr varchar NULL,
	sellingprice varchar NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT car_sales_pk PRIMARY KEY (id_sales)
);
