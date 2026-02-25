-- 1. Количество фильмов в каждой категории

select
    c.category_id,
    c.name,
    count(fc.film_id) as film_count
from category c
join film_category fc 
    on c.category_id = fc.category_id
group by
    c.category_id,
    c.name
order by film_count desc;



-- 2. Топ-10 актеров по количеству аренд (без склейки однофамильцев, оптимизированные join)

select
    a.actor_id,
    a.first_name || ' ' || a.last_name as name,
    count(r.rental_id) as rental_count
from actor a
join film_actor fa 
    on a.actor_id = fa.actor_id
join inventory i 
    on fa.film_id = i.film_id
join rental r 
    on i.inventory_id = r.inventory_id
group by
    a.actor_id,
    a.first_name,
    a.last_name
order by rental_count desc
limit 10;



-- 3. Категория фильмов с максимальной суммой оплат (без лишних join)

select
    c.category_id,
    c.name,
    sum(p.amount) as total_amount
from payment p
join rental r 
    on p.rental_id = r.rental_id
join inventory i 
    on r.inventory_id = i.inventory_id
join film_category fc 
    on i.film_id = fc.film_id
join category c 
    on fc.category_id = c.category_id
group by
    c.category_id,
    c.name
order by total_amount desc
limit 1;



-- 4. Фильмы, которых нет в inventory (без IN)

select
    f.title
from film f
where not exists (
    select 1
    from inventory i
    where i.film_id = f.film_id
);



-- 5. Топ-3 актеров в категории 'Children'
-- корректно работает даже если актеров меньше трех
-- выводит всех при равенстве

with actor_counts as (
    select
        a.actor_id,
        a.first_name || ' ' || a.last_name as name,
        count(*) as film_count
    from actor a
    join film_actor fa 
        on a.actor_id = fa.actor_id
    join film_category fc 
        on fa.film_id = fc.film_id
    join category c 
        on fc.category_id = c.category_id
    where c.name = 'Children'
    group by
        a.actor_id,
        a.first_name,
        a.last_name
)
select
    name,
    film_count
from (
    select
        name,
        film_count,
        dense_rank() over (order by film_count desc) as rnk
    from actor_counts
) t
where rnk <= 3
order by film_count desc;



-- 6. Города с количеством активных и неактивных клиентов

select
    ci.city,
    count(*) filter (where cu.active = 1) as active_customers,
    count(*) filter (where cu.active = 0) as inactive_customers
from customer cu
join address a 
    on cu.address_id = a.address_id
join city ci 
    on a.city_id = ci.city_id
group by ci.city
order by inactive_customers desc;



-- 7. Категория с максимальными часами аренды
-- для городов на "a" и содержащих "-"

with rental_hours as (
    select
        c.city as city_name,
        cat.name as category_name,
        sum(f.length) / 60.0 as total_hours
    from rental r
    join customer cu 
        on r.customer_id = cu.customer_id
    join address a 
        on cu.address_id = a.address_id
    join city c 
        on a.city_id = c.city_id
    join inventory i 
        on r.inventory_id = i.inventory_id
    join film f 
        on i.film_id = f.film_id
    join film_category fc 
        on f.film_id = fc.film_id
    join category cat 
        on fc.category_id = cat.category_id
    group by
        c.city,
        cat.name
)

select *
from (
    select
        'cities starting with a' as city_filter,
        city_name,
        category_name,
        total_hours,
        dense_rank() over (
            partition by city_name
            order by total_hours desc
        ) as rnk
    from rental_hours
    where city_name ilike 'a%'
) t
where rnk = 1

union all

select *
from (
    select
        'cities containing -' as city_filter,
        city_name,
        category_name,
        total_hours,
        dense_rank() over (
            partition by city_name
            order by total_hours desc
        ) as rnk
    from rental_hours
    where city_name like '%-%'
) t2
where rnk = 1;