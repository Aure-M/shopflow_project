create or replace file format SHOPFLOW_DB.RAW.FF_JSON
  type = 'JSON'
  strip_outer_array = true
  null_if = ('NULL', 'null', '')
  comment = 'Format JSON';


create or replace table SHOPFLOW_DB.RAW.PRODUCTS
(
    product_id VARCHAR(20) primary key not null,
    name VARCHAR(50),
    category VARCHAR(30),
    brand VARCHAR(30),
    price DECIMAL,
    attributes VARIANT,
    tags ARRAY
);


create or replace table SHOPFLOW_DB.RAW.WEB_EVENTS
(
    event VARIANT
);

-- Products
copy into SHOPFLOW_DB.RAW.PRODUCTS
FROM (
    SELECT
        $1:product_id::VARCHAR,
        $1:name::VARCHAR,
        $1:category::VARCHAR,
        $1:brand::VARCHAR,
        $1:price::DECIMAL,
        $1:attributes::VARIANT,
        $1:tags::ARRAY
    FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/products.json
    (file_format => 'SHOPFLOW_DB.RAW.FF_JSON')
);

-- Web events
copy into SHOPFLOW_DB.RAW.WEB_EVENTS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/web_events.json
file_format = 'SHOPFLOW_DB.RAW.FF_JSON';