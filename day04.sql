drop materialized view if exists search_dirs;
drop materialized view if exists word_search;
drop table if exists day04_input;

create table day04_input (i text);

insert into day04_input (i) values
...

create materialized view word_search as
    select r, c, sym[1] sym from (
        select row_number() over () as r, i from day04_input
    )_
cross join lateral regexp_matches(i, '.', 'g')
with ordinality as a(sym, c)

select string_agg(sym, '') from word_search
group by r
order by r

create materialized view search_dirs as
with directions as (
SELECT * FROM
   (VALUES
     ('N',  -1,  0)
   , ('NE', -1,  1)
   , ('E',   0,  1)
   , ('SE',  1,  1)
   , ('S',   1,  0)
   , ('SW',  1, -1)
   , ('W',   0, -1)
   , ('NW', -1, -1)
   )
   AS t(dir, rd, cd)
)
select row_number() over() as ix, * from directions
join word_search on sym = 'X'

-- | Part I
with recursive gogo as (
select ix
     , 1 as step_no
     , 'X' as curr_sym
     , r as curr_r
     , c as curr_c
     , dir
     , rd
     , cd
  from search_dirs
 union all
select ix
     , step_no + 1 as step_no
     , sym as curr_sym
     , r as curr_r
     , c as curr_c
     , dir
     , rd
     , cd
  from gogo
  join word_search on r = curr_r + rd and c = curr_c + cd
   and sym = case step_no when 1 then 'M'
                          when 2 then 'A'
                          when 3 then 'S'
                          else 'NO JOIN' end
)
select count(*) as part1
  from gogo
 where step_no = 4


-- | Part II
with start_a as (
select row_number() over() as ix, *
  from word_search where sym = 'A'
)
select count(*) as part2
  from (
    select ix
         , a.r
         , a.c
      from start_a a
      left join word_search nw on nw.r = a.r - 1 and nw.c = a.c - 1
      left join word_search se on se.r = a.r + 1 and se.c = a.c + 1
     where (nw.sym = 'M' and se.sym = 'S')
        or (nw.sym = 'S' and se.sym = 'M')
) a
  left join word_search ne on ne.r = a.r - 1 and ne.c = a.c + 1
  left join word_search sw on sw.r = a.r + 1 and sw.c = a.c - 1
 where (ne.sym = 'M' and sw.sym = 'S')
    or (ne.sym = 'S' and sw.sym = 'M')
