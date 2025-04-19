
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE public.us_state (
	id_state uuid DEFAULT uuid_generate_v4() NOT NULL,
	id_state_nk int4 NOT NULL,
	code varchar NULL,
	"name" varchar NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT us_state_pk PRIMARY KEY (id_state),
	CONSTRAINT us_state_unique UNIQUE (id_state_nk)
);
ALTER TABLE public.us_state OWNER TO postgres;
CREATE TABLE public.car_sales (
	id_sales uuid DEFAULT uuid_generate_v4() NOT NULL,
	id_sales_nk int4 NULL,
	"year" int4 NULL,
	brand_car_id uuid,
	transmission varchar NULL,
	id_state uuid,
	"condition" float4 NULL,
	odometer float4 NULL,
	color varchar NULL,
	interior varchar NULL,
	mmr float4 NULL,
	selling_price float4 NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT car_sales_pk PRIMARY KEY (id_sales),
	CONSTRAINT car_sales_unique UNIQUE (id_sales_nk)
);
ALTER TABLE public.car_sales OWNER TO postgres;
CREATE TABLE public.car_brand (
	brand_car_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	brand_car_id_nk int4 NOT NULL,
	brand_name varchar NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT car_brand_pk PRIMARY KEY (brand_car_id),
	CONSTRAINT car_brand_unique UNIQUE (brand_car_id_nk)
);
ALTER TABLE public.car_brand OWNER TO postgres;


ALTER TABLE ONLY public.car_sales
    ADD CONSTRAINT car_sales_fk_1 FOREIGN KEY (id_state) REFERENCES public.us_state(id_state);


ALTER TABLE ONLY public.car_sales
    ADD CONSTRAINT car_sales_fk_2 FOREIGN KEY (brand_car_id) REFERENCES public.car_brand(brand_car_id);
