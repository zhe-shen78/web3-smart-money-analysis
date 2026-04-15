with params as (
  select
    'FET' as token_symbol,
    'ethereum' as chain_name,
    lower('0xaea46a60368a7bd060eec7df8cba43b7ef41ad85') as evm_contract,
    45 as lookback_days,
    10 as min_active_days,
    1000 as min_net_flow_usd
),
flows as (
  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    lower(concat('0x', to_hex(t."to"))) as address_key,
    cast(t.amount as double) as flow_token,
    coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens.transfers t
    on t.blockchain = p.chain_name
   and t.contract_address = from_hex(substring(p.evm_contract, 3))
  where t."to" is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())

  union all

  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    lower(concat('0x', to_hex(t."from"))) as address_key,
    -cast(t.amount as double) as flow_token,
    -coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens.transfers t
    on t.blockchain = p.chain_name
   and t.contract_address = from_hex(substring(p.evm_contract, 3))
  where t."from" is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())
),
address_agg as (
  select
    token_symbol,
    chain_name,
    address_key,
    max(d) as as_of_date,
    count(distinct d) as active_days,
    min(case when flow_token > 0 then d end) as first_buy_day,
    sum(flow_token) as net_position_token,
    sum(flow_usd) as net_flow_usd,
    sum(case when flow_token > 0 then flow_token else 0 end) as buy_token_sum,
    sum(case when flow_usd > 0 then flow_usd else 0 end) as buy_usd_sum
  from flows
  where address_key <> '0x0000000000000000000000000000000000000000'
  group by 1,2,3
),
eligible as (
  select a.*
  from address_agg a
  join params p
    on a.token_symbol = p.token_symbol
   and a.chain_name = p.chain_name
  where a.active_days >= p.min_active_days
    and a.net_flow_usd > 0
    and a.net_position_token > 0
    and a.buy_token_sum > 0
),
thresholds as (
  select approx_percentile(net_flow_usd, 0.8) as p80_net_flow_usd
  from eligible
),
candidates as (
  select e.*
  from eligible e
  cross join thresholds t
  cross join params p
  where e.net_flow_usd >= t.p80_net_flow_usd
    and e.net_flow_usd >= p.min_net_flow_usd
),
price_latest as (
  select max_by(price, minute) as token_price_usd
  from prices.usd
  where symbol = 'FET'
    and minute >= date_add('day', -(select lookback_days from params), now())
)
select
  c.token_symbol,
  c.chain_name,
  c.as_of_date,
  c.address_key,
  c.active_days,
  c.first_buy_day,
  date_diff('day', c.first_buy_day, c.as_of_date) as hold_days_est,
  c.net_position_token,
  c.net_flow_usd,
  c.buy_token_sum,
  c.buy_usd_sum,
  c.buy_usd_sum / nullif(c.buy_token_sum, 0) as avg_buy_price_usd,
  p.token_price_usd,
  c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0)) as position_cost_usd_est,
  c.net_position_token * p.token_price_usd as position_value_usd,
  c.net_position_token * p.token_price_usd - c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0)) as unrealized_pnl_usd,
  (
    c.net_position_token * p.token_price_usd - c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0))
  ) / nullif(c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0)), 0) as unrealized_pnl_pct,
  100 * (
    c.net_position_token * p.token_price_usd - c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0))
  ) / nullif(c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0)), 0) as unrealized_pnl_pct_100
from candidates c
cross join price_latest p
order by position_cost_usd_est desc
limit 300
