-- DOMAIN: public.merchant_id

-- DROP DOMAIN public.merchant_id;

CREATE DOMAIN public.merchant_id
    AS integer
    NOT NULL;

ALTER DOMAIN public.merchant_id OWNER TO kris;

ALTER DOMAIN public.merchant_id
    ADD CONSTRAINT positive_merchant_id CHECK (VALUE > 0);

-- DOMAIN: public.offer_id

-- DROP DOMAIN public.offer_id;

CREATE DOMAIN public.offer_id
    AS integer
    NOT NULL;

ALTER DOMAIN public.offer_id OWNER TO kris;

ALTER DOMAIN public.offer_id
    ADD CONSTRAINT positive_offer_id CHECK (VALUE > 0);

-- DOMAIN: public.product_name

-- DROP DOMAIN public.product_name;

CREATE DOMAIN public.product_name
    AS character varying(200)
    NOT NULL;

ALTER DOMAIN public.product_name OWNER TO kris;

ALTER DOMAIN public.product_name
    ADD CONSTRAINT "not empty" CHECK (VALUE::text <> ''::text);

-- DOMAIN: public.product_price

-- DROP DOMAIN public.product_price;

CREATE DOMAIN public.product_price
    AS money
    NOT NULL;

ALTER DOMAIN public.product_price OWNER TO kris;

ALTER DOMAIN public.product_price
    ADD CONSTRAINT positive_product_price CHECK (VALUE > 0::money);

-- DOMAIN: public.product_quantity

-- DROP DOMAIN public.product_quantity;

CREATE DOMAIN public.product_quantity
    AS integer
    NOT NULL;

ALTER DOMAIN public.product_quantity OWNER TO kris;

ALTER DOMAIN public.product_quantity
    ADD CONSTRAINT positive_quantity CHECK (VALUE > 0);

-- Table: public.products

-- DROP TABLE public.products;

CREATE TABLE public.products
(
    merchant_id merchant_id,
    offer_id offer_id,
    name product_name COLLATE pg_catalog."default",
    price product_price,
    quantity product_quantity,
    CONSTRAINT unique_ids_pair UNIQUE (merchant_id, offer_id)
)

    TABLESPACE pg_default;

ALTER TABLE public.products
    OWNER to kris;