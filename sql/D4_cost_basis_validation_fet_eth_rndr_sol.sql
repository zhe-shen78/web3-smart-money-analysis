with params as (
  select
    lower('0xaea46a60368a7bd060eec7df8cba43b7ef41ad85') as evm_contract,
    45 as lookback_days,
    10 as min_active_days,
    1000 as min_net_flow_usd
),
raw_transfers as (
  select
    date_trunc('day', t.block_time) as d,
    t."from" as from_address,
    t."to" as to_address,
    cast(t.amount as double) as amount_token,
    coalesce(cast(t.amount_usd as double), 0) as amount_usd
  from params p
  join tokens.transfers t
    on t.blockchain = 'ethereum'
   and t.contract_address = from_hex(substring(p.evm_contract, 3))
  where t.block_time >= date_add('day', -p.lookback_days, now())
),
flows as (
  select
    d,
    lower(concat('0x', to_hex(to_address))) as address_key,
    amount_token as flow_token,
    amount_usd as flow_usd
  from raw_transfers
  where to_address is not null

  union all

  select
    d,
    lower(concat('0x', to_hex(from_address))) as address_key,
    -amount_token as flow_token,
    -amount_usd as flow_usd
  from raw_transfers
  where from_address is not null
),
daily_address_flows as (
  select
    d,
    address_key,
    sum(case when flow_token > 0 then flow_token else 0 end) as buy_token_day,
    sum(case when flow_usd > 0 then flow_usd else 0 end) as buy_usd_day,
    sum(case when flow_token < 0 then -flow_token else 0 end) as sell_token_day,
    sum(case when flow_usd < 0 then -flow_usd else 0 end) as sell_usd_day,
    sum(flow_token) as net_token_day,
    sum(flow_usd) as net_usd_day
  from flows
  where address_key <> '0x0000000000000000000000000000000000000000'
  group by 1, 2
),
address_summary as (
  select
    address_key,
    min(d) as first_active_day,
    max(d) as last_active_day,
    count(*) as active_days,
    sum(net_token_day) as net_position_token,
    sum(net_usd_day) as net_flow_usd,
    sum(buy_token_day) as buy_token_sum,
    sum(buy_usd_day) as buy_usd_sum
  from daily_address_flows
  group by 1
),
eligible as (
  select s.*
  from address_summary s
  cross join params p
  where s.active_days >= p.min_active_days
    and s.net_flow_usd >= p.min_net_flow_usd
    and s.net_position_token > 0
    and s.buy_token_sum > 0
),
thresholds as (
  select
    approx_percentile(net_flow_usd, 0.8) as p80_net_flow_usd,
    approx_percentile(net_flow_usd, 0.5) as p50_net_flow_usd
  from eligible
),
candidate_kpi as (
  select
    count(distinct address_key) as fet_candidate_count_kpi
  from eligible
),
daily_state as (
  select
    f.address_key,
    f.d,
    f.buy_token_day,
    f.buy_usd_day,
    f.sell_token_day,
    f.sell_usd_day,
    f.net_token_day,
    f.net_usd_day,
    sum(f.net_token_day) over (
      partition by f.address_key
      order by f.d
      rows between unbounded preceding and current row
    ) as position_token_eod,
    sum(f.net_usd_day) over (
      partition by f.address_key
      order by f.d
      rows between unbounded preceding and current row
    ) as cumulative_net_flow_usd_eod,
    sum(f.buy_token_day) over (
      partition by f.address_key
      order by f.d
      rows between unbounded preceding and current row
    ) as cumulative_buy_token,
    sum(f.buy_usd_day) over (
      partition by f.address_key
      order by f.d
      rows between unbounded preceding and current row
    ) as cumulative_buy_usd
  from daily_address_flows f
)
select
  'FET' as token_symbol,
  'ethereum' as chain_name,
  ds.d,
  ds.address_key,
  e.first_active_day,
  e.last_active_day,
  e.active_days,
  case
    when e.net_flow_usd >= t.p80_net_flow_usd then 'p80_plus'
    when e.net_flow_usd >= t.p50_net_flow_usd then 'p50_to_p80'
    else 'below_p50'
  end as net_flow_tier,
  ds.buy_token_day,
  ds.buy_usd_day,
  ds.sell_token_day,
  ds.sell_usd_day,
  ds.net_token_day,
  ds.net_usd_day,
  ds.position_token_eod,
  ds.cumulative_net_flow_usd_eod,
  ds.cumulative_buy_token,
  ds.cumulative_buy_usd,
  ds.cumulative_buy_usd / nullif(ds.cumulative_buy_token, 0) as running_buy_price_usd,
  k.fet_candidate_count_kpi,
  e.net_position_token as address_net_position_token_lookback,
  e.net_flow_usd as address_net_flow_usd_lookback,
  e.buy_usd_sum / nullif(e.buy_token_sum, 0) as address_avg_buy_price_usd_lookback
from daily_state ds
join eligible e
  on ds.address_key = e.address_key
cross join thresholds t
cross join candidate_kpi k
order by e.net_flow_usd desc, ds.address_key, ds.d
