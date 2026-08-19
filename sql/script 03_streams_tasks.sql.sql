-- ============================================================================
-- CRÉATION DES STREAMS (Schéma RAW)
-- ============================================================================
CREATE OR REPLACE STREAM SHOPFLOW_DB.RAW.STR_ORDERS ON TABLE SHOPFLOW_DB.RAW.ORDERS;
CREATE OR REPLACE STREAM SHOPFLOW_DB.RAW.STR_ORDER_ITEMS ON TABLE SHOPFLOW_DB.RAW.ORDER_ITEMS;
CREATE OR REPLACE STREAM SHOPFLOW_DB.RAW.STR_WEB_EVENTS ON TABLE SHOPFLOW_DB.RAW.WEB_EVENTS;


-- ============================================================================
-- CRÉATION DES TABLES CIBLES (Schéma STAGING)
-- ============================================================================
CREATE OR REPLACE TABLE SHOPFLOW_DB.STAGING.STG_CUSTOMERS
(
    customer_id VARCHAR(20) NOT NULL ,
    city        VARCHAR(40),
    email       VARCHAR(40),
    first_name  VARCHAR(40),
    last_name   VARCHAR(40),
    signup_date DATE
);

CREATE OR REPLACE TABLE SHOPFLOW_DB.STAGING.STG_PRODUCTS
(
    product_id  VARCHAR(20) NOT NULL ,
    name        VARCHAR(50),
    category    VARCHAR(30),
    brand       VARCHAR(30),
    price       DECIMAL(10, 2)
);

CREATE OR REPLACE TABLE SHOPFLOW_DB.STAGING.STG_ORDERS
(
    order_id     VARCHAR(20) NOT NULL ,
    customer_id  VARCHAR(20) NOT NULL,
    order_date   DATETIME,
    status       VARCHAR(20),
    total_amount DECIMAL(10, 2)
);

CREATE OR REPLACE TABLE SHOPFLOW_DB.STAGING.STG_ORDER_ITEMS
(
    order_id   VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity   INTEGER,
    unit_price DECIMAL(10, 2)
);

-- Table STG_WEB_EVENTS (Squelette à compléter)
CREATE OR REPLACE TABLE SHOPFLOW_DB.STAGING.STG_WEB_EVENTS
(
    event_id VARCHAR(20) NOT NULL,
    ip VARCHAR(20),
    referrer VARCHAR(20),
    device VARCHAR(20),
    event_type VARCHAR(20),
    product_id VARCHAR(20),
    session_id VARCHAR(50),
    user_id VARCHAR(20),
    timestamp DATETIME
);


-- ========================
--CRÉATION DU DAG DE TASKS 
-- =========================

-- Root
CREATE OR REPLACE TASK SHOPFLOW_DB.STAGING.TSK_ROOT_PIPELINE
    WAREHOUSE = WH_TRANSFORM
    SCHEDULE = '5 MINUTE'
AS
    SELECT CURRENT_TIMESTAMP();


-- Root >> Orders
CREATE OR REPLACE TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDERS
    WAREHOUSE = WH_TRANSFORM
    AFTER SHOPFLOW_DB.STAGING.TSK_ROOT_PIPELINE
    WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')
AS
MERGE INTO SHOPFLOW_DB.STAGING.STG_ORDERS tgt
USING (
    SELECT 
        order_id,
        customer_id,
        order_date,
        UPPER(TRIM(status)) AS status,
        total_amount,
        METADATA$ACTION AS meta_action,
        METADATA$ISUPDATE AS meta_isupdate
    FROM SHOPFLOW_DB.RAW.STR_ORDERS
    WHERE order_id IS NOT NULL 
      AND customer_id IS NOT NULL
      AND order_date <= CURRENT_TIMESTAMP()
      AND (total_amount IS NULL OR total_amount >= 0)
) src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.meta_action = 'DELETE' AND src.meta_isupdate = FALSE THEN 
    DELETE
WHEN MATCHED AND src.meta_action = 'INSERT' AND src.meta_isupdate = TRUE THEN 
    UPDATE SET 
        tgt.customer_id  = src.customer_id,
        tgt.order_date   = src.order_date,
        tgt.status       = src.status,
        tgt.total_amount = src.total_amount
WHEN NOT MATCHED AND src.meta_action = 'INSERT' THEN 
    INSERT (order_id, customer_id, order_date, status, total_amount)
    VALUES (src.order_id, src.customer_id, src.order_date, src.status, src.total_amount);


-- Root >> Web Events
CREATE OR REPLACE TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_WEB_EVENTS
    WAREHOUSE = WH_TRANSFORM
    AFTER SHOPFLOW_DB.STAGING.TSK_ROOT_PIPELINE
    WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')
AS
INSERT INTO SHOPFLOW_DB.STAGING.STG_WEB_EVENTS (
    event_id,
    ip,
    referrer,
    device,
    event_type,
    product_id,
    session_id,
    user_id,
    timestamp
)
SELECT 
    event:event_id::VARCHAR(20)         AS event_id,
    event:context:ip::VARCHAR(20)       AS ip,
    event:context:referrer::VARCHAR(50) AS referrer,
    event:device::VARCHAR(20)           AS device,
    event:event_type::VARCHAR(20)       AS event_type,
    event:product_id::VARCHAR(20)       AS product_id,
    event:session_id::VARCHAR(50)       AS session_id,
    event:user_id::VARCHAR(20)          AS user_id,
    event:timestamp::DATETIME           AS timestamp
FROM SHOPFLOW_DB.RAW.STR_WEB_EVENTS
WHERE METADATA$ACTION = 'INSERT'
  AND event:event_id IS NOT NULL
  AND event:timestamp::DATETIME <= CURRENT_TIMESTAMP();

-- Orders >> Orders Items
CREATE OR REPLACE TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDER_ITEMS
    WAREHOUSE = WH_TRANSFORM
    AFTER SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDERS
    WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS')
AS
MERGE INTO SHOPFLOW_DB.STAGING.STG_ORDER_ITEMS tgt
USING (
    SELECT 
        order_id,
        product_id,
        quantity,
        unit_price,
        METADATA$ACTION AS meta_action,
        METADATA$ISUPDATE AS meta_isupdate
    FROM SHOPFLOW_DB.RAW.STR_ORDER_ITEMS
    WHERE order_id IS NOT NULL 
      AND product_id IS NOT NULL
      AND (quantity IS NULL OR quantity > 0)
      AND (unit_price IS NULL OR unit_price >= 0)
) src
ON tgt.order_id = src.order_id AND tgt.product_id = src.product_id
WHEN MATCHED AND src.meta_action = 'DELETE' AND src.meta_isupdate = FALSE THEN 
    DELETE
WHEN MATCHED AND src.meta_action = 'INSERT' AND src.meta_isupdate = TRUE THEN 
    UPDATE SET 
        tgt.quantity   = src.quantity,
        tgt.unit_price = src.unit_price
WHEN NOT MATCHED AND src.meta_action = 'INSERT' THEN 
    INSERT (order_id, product_id, quantity, unit_price)
    VALUES (src.order_id, src.product_id, src.quantity, src.unit_price);


-- ============================================================================
-- ACTIVATION DU DAG (Ordre Bottom-Up obligatoire)
-- ============================================================================
ALTER TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDER_ITEMS RESUME;
ALTER TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_WEB_EVENTS RESUME;
ALTER TASK SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDERS RESUME;

ALTER TASK SHOPFLOW_DB.STAGING.TSK_ROOT_PIPELINE RESUME;


-- ALTER TASK SHOPFLOW_DB.STAGING.TSK_ROOT_PIPELINE SUSPEND;


-- ======================================
-- Copie des Données J2
-- ======================================

-- Orders
copy into SHOPFLOW_DB.RAW.ORDERS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/orders_j2.csv
file_format = 'SHOPFLOW_DB.RAW.FF_CSV_ORDERS';

-- Order Items
copy into SHOPFLOW_DB.RAW.ORDER_ITEMS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/order_items_j2.csv
file_format = 'SHOPFLOW_DB.RAW.FF_CSV_ORDERS';

-- Web events
copy into SHOPFLOW_DB.RAW.WEB_EVENTS
FROM @SHOPFLOW_DB.RAW.STAGE_LANDING/web_events_j2.json
file_format = 'SHOPFLOW_DB.RAW.FF_JSON';



-- ============
-- Check Data 
-- ============

SELECT * FROM SHOPFLOW_DB.STAGING.STG_ORDERS;

SELECT * FROM SHOPFLOW_DB.RAW.ORDERS;


-- ===================================
-- Copie des données des autres Tables
-- ===================================

-- Products
INSERT INTO SHOPFLOW_DB.STAGING.STG_PRODUCTS (
    product_id,
    name,
    category,
    brand,
    price
)
SELECT 
    product_id,
    TRIM(name) AS name,
    TRIM(category) AS category,
    TRIM(brand) AS brand,
    price
FROM SHOPFLOW_DB.RAW.PRODUCTS
WHERE product_id IS NOT NULL
  AND (price IS NULL OR price >= 0);


-- Customers
INSERT INTO SHOPFLOW_DB.STAGING.STG_CUSTOMERS (
    customer_id,
    city,
    email,
    first_name,
    last_name,
    signup_date
)
SELECT 
    customer_id,
    TRIM(city) AS city,
    LOWER(TRIM(email)) AS email,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    signup_date
FROM SHOPFLOW_DB.RAW.CUSTOMERS
WHERE customer_id IS NOT NULL
  AND (signup_date IS NULL OR signup_date <= CURRENT_DATE());
