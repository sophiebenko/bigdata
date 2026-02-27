-- 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

select
	fc.category_id,
	c."name",
	count(film_id)
from
	film_category fc
left join category c on
	fc.category_id = c.category_id
group by
	fc.category_id,
	c."name"
order by
	count(film_id) desc;

-- 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

select
	a.first_name || ' ' || a.last_name as name,
	count(r.rental_id)
from
	rental r
left join inventory i on
	i.inventory_id = r.inventory_id
left join film f on
	i.film_id = f.film_id
left join film_actor fa on
	fa.film_id = f.film_id
left join actor a on
	a.actor_id = fa.actor_id
group by
	a.first_name || ' ' || a.last_name
order by
	count(r.rental_id) desc
limit 10;

---------------
-- что произойдёт если в базу попадут актёры с одинаковыми именем и фамилией?
-- нужно оптимизировать джойны
---------------

-- 3. Вывести категорию фильмов, на которую потратили больше всего денег.

select
	c."name",
	sum(p.amount)
from
	rental r
left join payment p on
	p.rental_id = r.rental_id
left join inventory i on
	i.inventory_id = r.inventory_id
left join film f on
	i.film_id = f.film_id
left join film_actor fa on
	fa.film_id = f.film_id
left join film_category fc on
	fc.film_id = f.film_id
left join category c on
	fc.category_id = c.category_id
group by
	c."name"
order by
	sum(p.amount) desc
limit 1;

---------------
-- нужно оптимизировать джойны
---------------

-- 4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

select
	f.title
from
	inventory i
right join film f on
	i.film_id = f.film_id
where
	i.film_id is null;

-- 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
--    Если у нескольких актеров одинаковое кол-во фильмов, вывести всех

with actor_counts as (
select
	a.actor_id,
	a.first_name || ' ' || a.last_name as name,
	count(f.film_id) as film_count
from
	actor a
join film_actor fa on
	a.actor_id = fa.actor_id
join film f on
	fa.film_id = f.film_id
join film_category fc on
	f.film_id = fc.film_id
join category c on
	fc.category_id = c.category_id
where
	c."name" = 'children'
group by
	a.actor_id,
	a.first_name,
	a.last_name
)
select
	name,
	film_count
from
	actor_counts
where
	film_count >= (
	select
		film_count
	from
		actor_counts
	order by
		film_count desc
    offset 2
	limit 1
)
order by
	film_count desc;

---------------
-- что произойдёт если актёров будет меньше трёх?
-- исползование подзапроса может замедлить выполнение запроса, попробуй решить дргуим способом
---------------

-- 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
-- Отсортировать по количеству неактивных клиентов по убыванию.

select
	ci.city,
	count(case when cu.active = 1 then 1 end) as active_customers,
	count(case when cu.active = 0 then 1 end) as inactive_customers
from
	customer cu
join address a on
	cu.address_id = a.address_id
join city ci on
	a.city_id = ci.city_id
group by
	ci.city
order by
	inactive_customers desc;

-- 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
--  и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

with rental_hours as (
select
	c.city as city_name,
	cat.name as category_name,
	sum(f.length) as total_hours
from
	rental r
join customer cu on
	r.customer_id = cu.customer_id
join address a on
	cu.address_id = a.address_id
join city c on
	a.city_id = c.city_id
join inventory i on
	r.inventory_id = i.inventory_id
join film f on
	i.film_id = f.film_id
join film_category fc on
	f.film_id = fc.film_id
join category cat on
	fc.category_id = cat.category_id
group by
	c.city,
	cat.name
)
select
	'cities starting with a' as city_filter,
	city_name,
	category_name,
	total_hours
from
	(
	select
		distinct on
		(city_name)
        city_name,
		category_name,
		total_hours
	from
		rental_hours
	where
		city_name ilike 'a%'
	order by
		city_name,
		total_hours desc
) as t
union all

select
	'cities containing -' as city_filter,
	city_name,
	category_name,
	total_hours
from
	(
	select
		distinct on
		(city_name)
        city_name,
		category_name,
		total_hours
	from
		rental_hours
	where
		city_name like '%-%'
	order by
		city_name,
		total_hours desc
) as t2;

