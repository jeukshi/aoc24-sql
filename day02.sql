drop materialized view if exists reports_safe_corrected;
drop materialized view if exists reports_corrected_candidates;
drop materialized view if exists reports_safe;
drop materialized view if exists reports;
drop table if exists day02_input;

create table day02_input ( i text);

insert into day02_input (i) values
...

create materialized view reports as
select
 rn, lvl, val::bigint
from (
  select row_number() over () rn, i from day02_input
) _
cross join lateral unnest(string_to_array(i, ' '))
         with ordinality a(val, lvl)

create materialized view reports_safe as
with reports_prev_val as (
    select *, lag(val) over (partition by rn order by lvl) prev_val
      from reports
     order by rn, lvl
), reports_val_diff as (
    select rn, lvl, val, prev_val, val - prev_val as val_diff
      from reports_prev_val
)
select * from (
    select 'd' as dir, rn, bool_and(val_diff is null or val_diff between -3 and -1) safe
      from reports_val_diff
     group by rn
    union all
    select 'i' as dir, rn, bool_and(val_diff is null or val_diff between 1 and 3) safe
      from reports_val_diff
     group by rn
)_

-- | Part I
select count(*) as part1 from reports_safe where safe = true;

create materialized view reports_corrected_candidates as
with lvls as (
    select generate_series(1, (select max(lvl) from reports)) as lvl_removed
)
select lvl_removed
     , r.rn as rn
     , lvl
     , val
     , lag(val) over (partition by lvl_removed, r.rn order by lvl) prev_val
  from reports r
  full outer join lvls on true
  where lvl_removed <> lvl
    and r.rn not in (select distinct(s.rn) from reports_safe s where safe = true)

  order by lvl_removed, r.rn, lvl

create materialized view reports_safe_corrected as
with reports_val_diff as (
    select lvl_removed, rn, lvl, val, prev_val, val - prev_val as val_diff
      from reports_corrected_candidates
)
select distinct(rn) from (
    select 'd' as dir, rn, bool_and(val_diff is null or val_diff between -3 and -1) safe
      from reports_val_diff
     group by lvl_removed, rn
    union all
    select 'i' as dir, rn, bool_and(val_diff is null or val_diff between 1 and 3) safe
      from reports_val_diff
     group by lvl_removed, rn
)_
where safe = true

-- | Part II
select count(*) as part2 from (
    select distinct(rn) from reports_safe_corrected
     union all
    select distinct(rn) from reports_safe where safe = true
)_
