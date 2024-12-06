drop materialized view if exists guard_path;
drop materialized view if exists lab_map;
drop table if exists day06_input;

create table day06_input ( i text);

insert into day06_input (i) values
...

create materialized view lab_map as
select r, c, sym[1] sym from (
        select row_number() over () as r, i from day06_input
    )_
cross join lateral regexp_matches(i, '.', 'g')
with ordinality as a(sym, c)

create index on lab_map(r);
create index on lab_map(c);

select string_agg(sym, '') from lab_map
group by r
order by r

create type dir as (
    dir text,
    dr bigint,
    dc bigint
)

create materialized view guard_path as
with recursive gogo as (
select 1 as step
     , r as vr
     , c as vc
     , row('N', -1, 0)::dir as d
  from lab_map
 where sym = '^'
union all
select step + 1
     , case sym when '#' then vr
                else r end as vr
     , case sym when '#' then vc
                else c end as vc
     , case sym when '#' then
                         case when (d).dir = 'N' then row('E',  0,  1)::dir
                              when (d).dir = 'E' then row('S',  1,  0)::dir
                              when (d).dir = 'S' then row('W',  0, -1)::dir
                              when (d).dir = 'W' then row('N', -1,  0)::dir
                              end
                else d end as dir
  from gogo
  join lab_map on r = vr + (d).dr and c = vc + (d).dc
)
select step as gstep, vr as gr, vc as gc, d as gd from gogo;


-- | Part I
select count(*) as part1 from (
    select distinct(gr, gc) from guard_path
)_

create index on guard_path(gr);
create index on guard_path(gc);
create index on guard_path(gd);

create type vis as (
    d text,
    r bigint,
    c bigint
);

-- | Part II
with recursive gogo as (
select vr
     , vc
     , d
     , obs_r
     , obs_c
     , '{}'::vis[]  as visited
     , is_cycle
  from (
select row_number() over () as ix
     , gr as vr
     , gc as vc
     , gd as d
     , r as obs_r
     , c as obs_c
     , false as is_cycle
  from guard_path
  join lab_map on r = gr + (gd).dr and c = gc + (gd).dc
 where sym <> '#'
)_
union all
select case when sym = '#' or (r = obs_r and c = obs_c) then vr
                else r end as vr
     , case when sym = '#' or (r = obs_r and c = obs_c) then vc
                else c end as vc
     , case when sym = '#' or (r = obs_r and c = obs_c) then
                         case when (d).dir = 'N' then row('E',  0,  1)::dir
                              when (d).dir = 'E' then row('S',  1,  0)::dir
                              when (d).dir = 'S' then row('W',  0, -1)::dir
                              when (d).dir = 'W' then row('N', -1,  0)::dir
                              end
                else d end as dir
      , obs_r
      , obs_c
      , case when sym = '#' or (r = obs_r and c = obs_c)
                 then array_cat(visited, array[row((d).dir, vr, vc)::vis])
             else visited end as visited
      , row((d).dir, vr, vc)::vis = any(visited) as iscycle
  from gogo
  join lab_map on r = vr + (d).dr and c = vc + (d).dc
  left join guard_path on gr = vr and gc = vc and (gd).dir = (d).dir
 where is_cycle = false
)
select count(*) as part2
  from (
    select distinct obs_r, obs_c from gogo
)
