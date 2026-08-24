-- Setup ShopFlow database, roles, and stages
-- Co-authored with CoCo
-- Création de la base de données, schémas
create database SHOPFLOW_DB;
use database SHOPFLOW_DB;
create schema if not exists RAW;
create schema if not exists STAGING;
create schema if not exists MARTS;

-- Création du rôle SHOPFLOW_ENGINEER avec les autorisations basiques
use role ACCOUNTADMIN;

-- Create the role
create role if not exists SHOPFLOW_ENGINEER;

-- Create Warehouse
create warehouse if not exists WH_INGEST
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

create warehouse if not exists WH_TRANSFORM
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Accès au rôle pour utiliser les warehouses
grant usage on warehouse WH_INGEST TO ROLE SHOPFLOW_ENGINEER;
grant usage on warehouse WH_TRANSFORM TO ROLE SHOPFLOW_ENGINEER;

-- Accès au rôle pour utiliser la base de données, les schémas, vues, tables, etc
grant all on database SHOPFLOW_DB TO ROLE SHOPFLOW_ENGINEER;
grant all on all schemas in database SHOPFLOW_DB to role SHOPFLOW_ENGINEER;

-- Associer le rôle SHOPFLOW_ENGINEER sous le rôle SYSADMIN
grant role SHOPFLOW_ENGINEER to role SYSADMIN;

use role SHOPFLOW_ENGINEER;



-- Création du stage de stockage des données brutes
create or replace stage SHOPFLOW_DB.RAW.STAGE_LANDING
  comment = 'Stage de stockage des données brutes';


-- Création des file formats

create or replace file format SHOPFLOW_DB.RAW.FF_CSV_ORDERS
  type = 'CSV'
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null', '')
  empty_field_as_null = true
  field_optionally_enclosed_by = '"'
  error_on_column_count_mismatch = false
  comment = 'Format CSV';

create or replace file format SHOPFLOW_DB.RAW.FF_PARQUET
  type = 'PARQUET'
  compression = 'SNAPPY'
  comment = 'Format Parquet';

create or replace file format SHOPFLOW_DB.RAW.FF_JSON
  type = 'JSON'
  strip_outer_array = true
  null_if = ('NULL', 'null', '')
  comment = 'Format JSON';

-------------------------------------------
-- Création des tables et copie des données
-------------------------------------------

-- Création des TABLES
use database SHOPFLOW_DB;
use schema RAW;
use role SHOPFLOW_ENGINEER;

list @SHOPFLOW_DB.RAW.STAGE_LANDING;


select $1
from @SHOPFLOW_DB.RAW.STAGE_LANDING/customers.parquet
(file_format => 'SHOPFLOW_DB.RAW.FF_PARQUET')
limit 10;

create or replace table SHOPFLOW_DB.RAW.CUSTOMERS
(
    customer_id VARCHAR(20) primary key not null,
    city VARCHAR(40),
    email VARCHAR(40),
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    signup_date DATE 
);

create or replace table SHOPFLOW_DB.RAW.ORDERS
(
    order_id VARCHAR(20) primary key not null,
    customer_id VARCHAR(20) foreign key references SHOPFLOW_DB.RAW.CUSTOMERS(customer_id),
    order_date DATETIME,
    status VARCHAR(20),
    total_amount DECIMAL
);

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


create or replace table SHOPFLOW_DB.RAW.ORDER_ITEMS
(
    order_id VARCHAR(20) foreign key references SHOPFLOW_DB.RAW.ORDERS(order_id),
    product_id VARCHAR(20) foreign key references SHOPFLOW_DB.RAW.PRODUCTS(product_id),
    quantity INTEGER,
    unit_price DECIMAL,
    constraint PK_Order_Items primary key (order_id, product_id)
);

create or replace table SHOPFLOW_DB.RAW.WEB_EVENTS
(
    event VARIANT
);


-- Copie des données 
use warehouse WH_INGEST;
-- Customers
copy into SHOPFLOW_DB.RAW.CUSTOMERS
FROM (
  SELECT 
    $1:customer_id::VARCHAR,
    $1:city::VARCHAR,
    $1:email::VARCHAR,
    $1:first_name::VARCHAR,
    $1:last_name::VARCHAR,
    $1:signup_date::DATE
  FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/customers.parquet
  (file_format => 'SHOPFLOW_DB.RAW.FF_PARQUET')
);

-- Orders
copy into SHOPFLOW_DB.RAW.ORDERS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/orders.csv
file_format = 'SHOPFLOW_DB.RAW.FF_CSV_ORDERS';

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

-- Order Items
copy into SHOPFLOW_DB.RAW.ORDER_ITEMS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/order_items.csv
file_format = 'SHOPFLOW_DB.RAW.FF_CSV_ORDERS';

-- Web events
copy into SHOPFLOW_DB.RAW.WEB_EVENTS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/web_events.json
file_format = 'SHOPFLOW_DB.RAW.FF_JSON';
