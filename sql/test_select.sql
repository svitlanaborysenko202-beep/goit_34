-- DDL: schemas, tables, keys, constraints
CREATE SCHEMA IF NOT EXISTS sales;

SELECT * FROM sales.customers
  LIMIT 10;

-- Parent table
CREATE TABLE IF NOT EXISTS sales.customers (
                                               customer_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                               email            VARCHAR(255) NOT NULL UNIQUE,
                                               full_name        VARCHAR(200) NOT NULL,
                                               created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                               status           VARCHAR(20) NOT NULL DEFAULT 'active',
                                               CHECK (status IN ('active', 'inactive'))
);

-- Child table referencing parent, with composite unique key
CREATE TABLE IF NOT EXISTS sales.orders (
                                            order_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                            customer_id      BIGINT NOT NULL,
                                            order_number     VARCHAR(50) NOT NULL,
                                            order_date       DATE NOT NULL DEFAULT CURRENT_DATE,
                                            total_amount     NUMERIC(12,2) NOT NULL DEFAULT 0,
                                            CONSTRAINT uq_orders_order_number UNIQUE (order_number),
                                            CONSTRAINT fk_orders_customer
                                                FOREIGN KEY (customer_id)
                                                    REFERENCES sales.customers(customer_id)
                                                    ON UPDATE CASCADE
                                                    ON DELETE RESTRICT
);

-- Junction table (many-to-many)
CREATE TABLE IF NOT EXISTS sales.products (
                                              product_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                              sku              VARCHAR(50) NOT NULL UNIQUE,
                                              name             VARCHAR(200) NOT NULL,
                                              price            NUMERIC(10,2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE IF NOT EXISTS sales.order_items (
                                                 order_id         BIGINT NOT NULL,
                                                 product_id       BIGINT NOT NULL,
                                                 quantity         INTEGER NOT NULL CHECK (quantity > 0),
                                                 unit_price       NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
                                                 PRIMARY KEY (order_id, product_id),
                                                 CONSTRAINT fk_items_order
                                                     FOREIGN KEY (order_id)
                                                         REFERENCES sales.orders(order_id)
                                                         ON UPDATE CASCADE
                                                         ON DELETE CASCADE,
                                                 CONSTRAINT fk_items_product
                                                     FOREIGN KEY (product_id)
                                                         REFERENCES sales.products(product_id)
                                                         ON UPDATE CASCADE
                                                         ON DELETE RESTRICT
);

-- Indexes (b-tree)
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON sales.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON sales.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON sales.customers(created_at);
CREATE INDEX IF NOT EXISTS idx_products_price ON sales.products(price);

-- Optional: partial or expression index (if supported)
CREATE INDEX IF NOT EXISTS idx_active_customers ON sales.customers(email) WHERE status = 'active';

-- Materialized view (aggregated sales by customer)
-- Note: Requires a DB that supports MATERIALIZED VIEW.
CREATE MATERIALIZED VIEW IF NOT EXISTS sales.mv_customer_revenue AS
SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS orders_count,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0)::NUMERIC(14,2) AS gross_revenue,
    MAX(o.order_date) AS last_order_date
FROM sales.orders o
         LEFT JOIN sales.order_items oi ON oi.order_id = o.order_id
GROUP BY o.customer_id
WITH NO DATA;

-- To populate/refresh the materialized view:
-- REFRESH MATERIALIZED VIEW sales.mv_customer_revenue;

-- Standard CTE (non-recursive): top customers by revenue
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        c.full_name,
        COALESCE(mv.gross_revenue, 0) AS gross_revenue
    FROM sales.customers c
             LEFT JOIN sales.mv_customer_revenue mv
                       ON mv.customer_id = c.customer_id
)
SELECT *
FROM customer_revenue
ORDER BY gross_revenue DESC, customer_id
LIMIT 10;

-- Example hierarchy table for recursive CTE
CREATE TABLE IF NOT EXISTS sales.categories (
                                                category_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                parent_id        BIGINT NULL,
                                                name             VARCHAR(200) NOT NULL,
                                                CONSTRAINT fk_categories_parent
                                                    FOREIGN KEY (parent_id)
                                                        REFERENCES sales.categories(category_id)
                                                        ON UPDATE CASCADE
                                                        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_categories_parent ON sales.categories(parent_id);

-- Recursive CTE: category tree with path and depth
WITH RECURSIVE category_tree AS (
    -- Anchor: root categories
    SELECT
        c.category_id,
        c.parent_id,
        c.name,
        1 AS depth,
        LPAD(CAST(c.category_id AS VARCHAR), 1, '0') AS path
    FROM sales.categories c
    WHERE c.parent_id IS NULL

    UNION ALL

    -- Recursive: children
    SELECT
        ch.category_id,
        ch.parent_id,
        ch.name,
        ct.depth + 1 AS depth,
        ct.path || '>' || CAST(ch.category_id AS VARCHAR) AS path
    FROM sales.categories ch
             JOIN category_tree ct
                  ON ch.parent_id = ct.category_id
)
SELECT *
FROM category_tree
ORDER BY path;

-- Recursive CTE: compute running totals for an order's items ordered by product_id
WITH ordered_items AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY oi.product_id) AS rn
    FROM sales.order_items oi
    WHERE oi.order_id = 1
),
     rec AS (
         SELECT
             order_id, product_id, quantity, unit_price, rn,
             (quantity * unit_price)::NUMERIC(14,2) AS line_total,
             (quantity * unit_price)::NUMERIC(14,2) AS running_total
         FROM ordered_items
         WHERE rn = 1

         UNION ALL

         SELECT
             o.order_id, o.product_id, o.quantity, o.unit_price, o.rn,
             (o.quantity * o.unit_price)::NUMERIC(14,2) AS line_total,
             (r.running_total + (o.quantity * o.unit_price))::NUMERIC(14,2) AS running_total
         FROM ordered_items o
                  JOIN rec r ON o.order_id = r.order_id AND o.rn = r.rn + 1
     )
SELECT order_id, product_id, quantity, unit_price, line_total, running_total
FROM rec
ORDER BY rn;
