drop materialized view if exists antinodes;
drop materialized view if exists roof_map;
drop table if exists day08_input;

create table day08_input ( i text);

insert into day08_input (i) values
...

create materialized view roof_map as
select r, c, sym[1] sym from (
        select row_number() over () as r, i from day08_input
    )_
cross join lateral regexp_matches(i, '.', 'g')
with ordinality as a(sym, c)

create index on roof_map(r);
create index on roof_map(c);

create materialized view antinodes as
select a.r + (a.r - b.r) as anti_r
     , a.c + (a.c - b.c) as anti_c
     , a.sym as anti_sym
  from roof_map a
  join roof_map b on a.sym = b.sym and a.sym <> '.'
 where not (a.r = b.r and a.c = b.c)

-- | Part I
select count(*) as part1 from (
    select distinct anti_r, anti_c
      from antinodes
      join roof_map on r = anti_r and c = anti_c
)_

-- | Part II
with recursive gogo as (
select a.r as anti_r
     , a.c as anti_c
     , (a.r - b.r) as dr
     , (a.c - b.c) as dc
     , a.sym as anti_sym
  from roof_map a
  join roof_map b on a.sym = b.sym and a.sym <> '.'
 where not (a.r = b.r and a.c = b.c)
union all
select r as anti_r
     , c as anti_c
     , dr
     , dc
     , anti_sym
  from gogo
  join roof_map on r = anti_r + dr and c = anti_c + dc
)
select count(*) as part2 from (
    select distinct anti_r, anti_c from gogo
)
