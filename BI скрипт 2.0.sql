-- ========================================
-- 0 Схемы
-- ========================================
drop schema if exists stage cascade;
create schema stage;

drop schema if exists core cascade;
create schema core;

drop schema if exists mart cascade;
create schema mart;

-- ========================================
-- 1 Stage Layer
-- ========================================
-- Stage orders_raw
drop table if exists stage.orders_raw;
create table stage.orders_raw (
    row_id INT,
    order_id VARCHAR(20),
    order_date TEXT,
    ship_date TEXT,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(20),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name TEXT,
    sales numeric(12, 2),
    quantity INT,
    discount numeric(5, 2),
    profit numeric(12, 2),
    load_dttm TIMESTAMP default CURRENT_TIMESTAMP
);

-- Stage orders_delta
drop table if exists stage.orders_delta;
create table stage.orders_delta (
    like stage.orders_raw including all
);

-- ========================================
-- 2 Core Layer
-- ========================================
-- Dim Product (SCD Type 1)
drop table if exists core.dim_product;
create table core.dim_product (
    product_key SERIAL primary key,
    product_id VARCHAR(20) unique not null,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name TEXT
);

-- Dim Customer (SCD Type 2)
drop table if exists core.dim_customer;
create table core.dim_customer (
    customer_key SERIAL primary key,
    customer_id VARCHAR(20) not null,
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    valid_from TIMESTAMP not null,           -- начало действия версии (бизнес‑время)
    valid_to TIMESTAMP,                      -- окончание действия, null если текущая
    is_current BOOLEAN not null default true
);

create index idx_dim_customer_business
on core.dim_customer(customer_id, is_current);

-- Dim Date
drop table if exists core.dim_date;
create table core.dim_date (
    date_key DATE primary key,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT
);

insert into core.dim_date
select
    d::DATE,
    extract(year from d),
    extract(QUARTER from d),
    extract(month from d),
    TO_CHAR(d, 'Month'),
    extract(day from d)
from generate_series('2014-01-01'::DATE, '2018-12-31'::DATE, '1 day') d;

-- Fact Orders
drop table if exists core.fact_orders;
create table core.fact_orders (
    order_id VARCHAR(20),
    order_date DATE,
    ship_date DATE,
    customer_key INT references core.dim_customer(customer_key),
    product_key INT references core.dim_product(product_key),
    sales numeric(12, 2),
    quantity INT,
    discount numeric(5, 2),
    profit numeric(12, 2)
);

-- ========================================
-- 3 Загрузка CSV в Stage
-- ========================================
-- Initial load
copy stage.orders_raw (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment, country, city, state, postal_code, region,
    product_id, category, sub_category, product_name,
    sales, quantity, discount, profit
)
from 'C:\Program Files\PostgreSQL\18\data\Superstore.csv'
delimiter ','
csv header;

-- Delta load
copy stage.orders_delta (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment, country, city, state, postal_code, region,
    product_id, category, sub_category, product_name,
    sales, quantity, discount, profit
)
from 'C:\Program Files\PostgreSQL\18\data\Superstore_delta.csv'
delimiter ','
csv header;

-- ========================================
-- 4 Core Load (Products SCD1) из всех данных
-- ========================================
-- Объединяем raw и delta, для каждого product_id берём последнюю запись по load_dttm
insert into core.dim_product (product_id, category, sub_category, product_name)
select
    product_id,
    category,
    sub_category,
    product_name
from (
    select
        product_id,
        category,
        sub_category,
        product_name,
        row_number() over (partition by product_id order by load_dttm desc) as rn
    from (
        select * from stage.orders_raw
        union all
        select * from stage.orders_delta
    ) all_orders
) t
where rn = 1
on conflict (product_id)
do update set
    category = EXCLUDED.category,
    sub_category = EXCLUDED.sub_category,
    product_name = EXCLUDED.product_name;

-- ========================================
-- 5 Core Load (Customers SCD2) 
-- ========================================

-- 5.1 Инициализация справочника клиентов из всех данных (raw + delta)
--     Для каждого customer_id берём самую позднюю запись по load_dttm,
--     а valid_from устанавливаем в минимальную дату заказа этого клиента.
insert into core.dim_customer (
    customer_id, customer_name, segment, country, city, state, postal_code, region,
    valid_from, valid_to, is_current
)
select
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    first_order_date,
    null,
    true
from (
    select
        customer_id,
        (array_agg(customer_name order by load_dttm desc))[1] as customer_name,
        (array_agg(segment order by load_dttm desc))[1] as segment,
        (array_agg(country order by load_dttm desc))[1] as country,
        (array_agg(city order by load_dttm desc))[1] as city,
        (array_agg(state order by load_dttm desc))[1] as state,
        (array_agg(postal_code order by load_dttm desc))[1] as postal_code,
        (array_agg(region order by load_dttm desc))[1] as region,
        min(to_date(order_date, 'MM/DD/YYYY')) as first_order_date
    from (
        select * from stage.orders_raw
        union all
        select * from stage.orders_delta
    ) all_orders
    group by customer_id
) init
on conflict do nothing;

-- 5.2 Обработка изменений из delta (SCD Type 2)
--     Создаём временную таблицу с изменениями, чтобы использовать её и для UPDATE, и для INSERT.
drop table if exists temp_changed_customers;
create temporary table temp_changed_customers as
select distinct
    d.customer_id,
    d.customer_name,
    d.segment,
    d.country,
    d.city,
    d.state,
    d.postal_code,
    d.region,
    min(to_date(d.order_date, 'MM/DD/YYYY')) as change_date
from stage.orders_delta d
join core.dim_customer c
    on d.customer_id = c.customer_id and c.is_current = true
where (c.customer_name <> d.customer_name
    or c.segment <> d.segment
    or c.country <> d.country
    or c.city <> d.city
    or c.state <> d.state
    or coalesce(c.postal_code, '') <> coalesce(d.postal_code, '')
    or c.region <> d.region)
group by d.customer_id, d.customer_name, d.segment, d.country, d.city, d.state, d.postal_code, d.region;

-- Закрываем старые версии
update core.dim_customer c
set
    valid_to = cc.change_date,
    is_current = false
from temp_changed_customers cc
where c.customer_id = cc.customer_id and c.is_current = true;

-- Вставляем новые версии с valid_from = change_date
insert into core.dim_customer (
    customer_id, customer_name, segment, country, city, state, postal_code, region,
    valid_from, valid_to, is_current
)
select
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    change_date,
    null,
    true
from temp_changed_customers;

-- 5.3 Добавляем совершенно новых клиентов из delta, которых ещё нет в dim_customer
insert into core.dim_customer (
    customer_id, customer_name, segment, country, city, state, postal_code, region,
    valid_from, valid_to, is_current
)
select
    d.customer_id,
    d.customer_name,
    d.segment,
    d.country,
    d.city,
    d.state,
    d.postal_code,
    d.region,
    min(to_date(d.order_date, 'MM/DD/YYYY')) as first_order_date,
    null,
    true
from stage.orders_delta d
left join core.dim_customer c
    on d.customer_id = c.customer_id
where c.customer_id is null
group by d.customer_id, d.customer_name, d.segment, d.country, d.city, d.state, d.postal_code, d.region;

-- ========================================
-- 6 Core Fact Orders
-- ========================================
insert into core.fact_orders (
    order_id, order_date, ship_date, customer_key, product_key,
    sales, quantity, discount, profit
)
select
    t.order_id,
    t.order_date,
    t.ship_date,
    t.customer_key,
    t.product_key,
    t.sales,
    t.quantity,
    t.discount,
    t.profit
from (
    select
        o.order_id,
        TO_DATE(o.order_date, 'MM/DD/YYYY') as order_date,
        TO_DATE(o.ship_date, 'MM/DD/YYYY') as ship_date,
        c.customer_key,
        p.product_key,
        o.sales,
        o.quantity,
        o.discount,
        o.profit,
        -- защита от дублей по row_id (берём последнюю загрузку)
        row_number() over (partition by o.row_id order by o.load_dttm desc) as rn
    from (
        select * from stage.orders_raw
        union all
        select * from stage.orders_delta
    ) o
    join core.dim_product p on o.product_id = p.product_id
    join core.dim_customer c
        on o.customer_id = c.customer_id
        and TO_DATE(o.order_date, 'MM/DD/YYYY') >= c.valid_from
        and (TO_DATE(o.order_date, 'MM/DD/YYYY') < c.valid_to or c.valid_to is null)
) t
where t.rn = 1;

-- ========================================
-- 7 Mart Layer (обновление)
-- ========================================
-- Очищаем и пересоздаём витрины на основе актуальных данных core

-- dim_product
drop table if exists mart.dim_product;
create table mart.dim_product as
select * from core.dim_product;

-- dim_customer (только текущие версии)
drop table if exists mart.dim_customer;
create table mart.dim_customer as
select * from core.dim_customer where is_current = true;

-- dim_date
drop table if exists mart.dim_date;
create table mart.dim_date as
select * from core.dim_date;

-- fact_sales
drop table if exists mart.fact_sales;
create table mart.fact_sales as
select
    f.order_id,
    f.order_date as date_key,
    f.customer_key,
    f.product_key,
    f.sales,
    f.quantity,
    f.discount,
    f.profit
from core.fact_orders f;