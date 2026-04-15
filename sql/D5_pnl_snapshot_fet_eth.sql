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
snapshot_day as (
  select max(d) as as_of_date
  from flows
),
address_agg as (
  select
    token_symbol,
    chain_name,
    address_key,
    max(s.as_of_date) as as_of_date,
    count(distinct d) as active_days,
    min(case when flow_token > 0 then d end) as first_buy_day,
    sum(flow_token) as net_position_token,
    sum(flow_usd) as net_flow_usd,
    sum(case when flow_token > 0 then flow_token else 0 end) as buy_token_sum,
    sum(case when flow_usd > 0 then flow_usd else 0 end) as buy_usd_sum
  from flows
  cross join snapshot_day s
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
),
pnl_snapshot as (
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
    100 * (
      c.net_position_token * p.token_price_usd - c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0))
    ) / nullif(c.net_position_token * (c.buy_usd_sum / nullif(c.buy_token_sum, 0)), 0) as unrealized_pnl_pct_100
  from candidates c
  cross join price_latest p
),
pnl_bucketed as (
  select
    s.*,
    case
      when s.unrealized_pnl_pct_100 < 0 then 'L1_loss'
      when s.unrealized_pnl_pct_100 < 10 then 'L2_0_10'
      when s.unrealized_pnl_pct_100 < 30 then 'L3_10_30'
      when s.unrealized_pnl_pct_100 < 60 then 'L4_30_60'
      else 'L5_60_plus'
    end as pnl_bucket
  from pnl_snapshot s
),
bucket_dim as (
  select 1 as bucket_order, 'L1_loss' as pnl_bucket, 'Loss (<0%)' as bucket_label, '#F08080' as bucket_color_hex
  union all select 2, 'L2_0_10', '0%-10%', '#8D99AE'
  union all select 3, 'L3_10_30', '10%-30%', '#5C7C8A'
  union all select 4, 'L4_30_60', '30%-60%', '#2A9D8F'
  union all select 5, 'L5_60_plus', '60%+', '#1B7F5A'
),
bucket_stats as (
  select
    pnl_bucket,
    count(*) as address_count,
    avg(unrealized_pnl_pct_100) as avg_pnl_pct_100,
    approx_percentile(unrealized_pnl_pct_100, 0.5) as p50_pnl_pct_100,
    sum(position_cost_usd_est) as total_position_cost_usd,
    sum(position_value_usd) as total_position_value_usd,
    sum(unrealized_pnl_usd) as total_unrealized_pnl_usd,
    avg(hold_days_est) as avg_hold_days
  from pnl_bucketed
  group by 1
),
group_totals as (
  select
    count(*) as total_address_count,
    sum(unrealized_pnl_usd) as group_total_unrealized_pnl_usd,
    100 * sum(case when unrealized_pnl_pct_100 >= 0 then 1 else 0 end) / nullif(count(*), 0) as group_win_rate_pct_100
  from pnl_bucketed
),
meta as (
  select
    'FET' as token_symbol,
    'ethereum' as chain_name,
    max(as_of_date) as as_of_date
  from pnl_snapshot
)
select
  m.token_symbol,
  m.chain_name,
  m.as_of_date,
  d.pnl_bucket,
  d.bucket_order,
  d.bucket_label,
  d.bucket_color_hex,
  coalesce(s.address_count, 0) as address_count,
  coalesce(s.address_count, 0) * 1.0 / nullif(g.total_address_count, 0) as address_share,
  100 * coalesce(s.address_count, 0) * 1.0 / nullif(g.total_address_count, 0) as address_share_pct_100,
  s.avg_pnl_pct_100,
  s.p50_pnl_pct_100,
  coalesce(s.total_position_cost_usd, 0) as total_position_cost_usd,
  coalesce(s.total_position_value_usd, 0) as total_position_value_usd,
  coalesce(s.total_unrealized_pnl_usd, 0) as total_unrealized_pnl_usd,
  s.avg_hold_days,
  g.group_total_unrealized_pnl_usd,
  g.group_win_rate_pct_100
from bucket_dim d
left join bucket_stats s
  on d.pnl_bucket = s.pnl_bucket
cross join group_totals g
cross join meta m
order by d.bucket_order
