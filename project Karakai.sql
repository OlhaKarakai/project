-- об'ЇднуЇмо дв≥ таблиц≥, групуЇмо за вс≥ма не агрегованими стовбц€ми, округлюЇмо дати до м≥с€ц≥в
with grouped_by_month as(
select
	gp.user_id,
	gp.game_name,
	age,
	language,
	has_older_device_model,
	date(date_trunc('month',
	payment_date)) as current_payment_month,
	sum(revenue_amount_usd) as revenue
from
	games_payments gp
join games_paid_users gpu on
	gp.user_id = gpu.user_id
group by
	date(date_trunc('month',
	payment_date)),
	gp.user_id,
	gp.game_name,
	age,
	language,
	has_older_device_model
order by
	user_id,
	date(date_trunc('month',
	payment_date))),

-- використовуючи в≥конн≥ функц≥њ lag, lead знаходимо допом≥жн≥ стовбц≥ дл€ обрахунку метрик
expanded_data as (
select
	gm.*,
	lag(revenue, 1)over (partition by user_id order by current_payment_month) as previous_revenue,
	revenue-lag(revenue, 1)over (partition by user_id order by current_payment_month) as revenue_diff,
	lag(current_payment_month, 1) over (partition by user_id order by current_payment_month) as previous_payment_month,
	lead(current_payment_month,	1) over (partition by user_id order by current_payment_month) as next_payment_month,
	date(date_trunc('month', current_payment_month - interval '1 month')) as calendar_month_minus_1,
	date(date_trunc('month', current_payment_month + interval '1 month')) as calendar_month_plus_1
from
	grouped_by_month gm),

-- знаходимо м≥с€ць в котрому користувач перестав платити (churn month) через умову case та п≥дзапити
mmr_data as (
select
	ed.*,
	case
		when current_payment_month != (select max(current_payment_month) from expanded_data)
		and next_payment_month is null
		or
		current_payment_month != (select max(current_payment_month) from expanded_data)
		and date_part('month', age(next_payment_month, current_payment_month))>1 
then 1
	end churn_month
from
	expanded_data ed),

-- використовуючи UNION додаЇмо нов≥ р€дки з м≥с€цем наступним п≥сл€ churn month
union_tables as (
select
	md.*,
	'existed' as added_mark
from
	mmr_data md
union all
select
	user_id,
	game_name,
	age,
	language,
	has_older_device_model,
case
	when churn_month = 1 then date(date_trunc('month', current_payment_month + interval '1 month'))
end as current_payment_month,
	0 as revenue,
	previous_revenue,
	revenue_diff,
	previous_payment_month,
	next_payment_month,
	calendar_month_minus_1,
	calendar_month_plus_1,
	0 as churn_month,
	'added' as added_mark
from
	mmr_data md
where
	churn_month = 1),

-- розраховуЇмо необх≥дн≥ метрики через оператор case
calculation_table as (
select
	ut.*,
	case
		when added_mark = 'added' then 1
	end churned_users,
	
	case
		when next_payment_month is not null
			and date_part('month', age(current_payment_month, previous_payment_month))>1
			and added_mark != 'added'
			or
			next_payment_month is null
			and date_part('month', age(current_payment_month, previous_payment_month))>1
			and added_mark != 'added'
		then 1
	end returned_from_churn,
				
	case
		when previous_payment_month is null
			and added_mark = 'existed' then 1
	end as new_users,
				
	case
		when previous_payment_month = calendar_month_minus_1
			and revenue > previous_revenue
			and added_mark = 'existed' then revenue
	end expansion_mrr,
				
	case
		when previous_payment_month = calendar_month_minus_1
			and revenue < previous_revenue
			and added_mark = 'existed' then revenue
	end contraction_mrr,
				
	case
		when churn_month = 1 then revenue
	end churned_revenue,
				
	lag(revenue, 1) over (partition by user_id order by current_payment_month) as previous_revenue_new
				
	from
		union_tables ut
	order by
		user_id,
		current_payment_month),

-- додаЇмо стовбець Change factors
change_factors as (
select
	ct.*,
	case
		when new_users = 1 then 'New Users'
		when expansion_mrr is not null then 'Revenue Expansion'
		when contraction_mrr is not null then 'Revenue Contraction'
		when churned_users is not null then 'Churn'
		when returned_from_churn is not null then 'Back from Churn'
		else 'No Change'
	end as change_factors
from
	calculation_table ct)

-- виводимо всю табицю	
select
	*
from
	change_factors



--select current_payment_month, sum(churned_users) churn, count(case when added_mark = 'existed' then user_id end) TOTAL, sum(new_users) new, sum(returned_from_churn) returned from change_factors 
--group by current_payment_month
--order by 1





