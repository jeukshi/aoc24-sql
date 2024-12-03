drop materialized view if exists list2;
drop materialized view if exists list1;
drop table if exists day01_input;

create table day01_input ( i text);

insert into day01_input (i) values
...

create materialized view list1 as
    select row_number() over (order by i) rn, i from (
        select split_part(i, '   '::text, 1)::bigint as i
          from day01_input
    ) _

create materialized view list2 as
    select row_number() over (order by i) rn, i from (
        select split_part(i, '   '::text, 2)::bigint as i
        from day01_input
    ) _

-- | Part I
select sum(abs(list1.i - list2.i)) as part1
  from list1
  join list2 on list1.rn = list2.rn


-- | Part II
select sum(i * c) as part2 from (
    select list1.i, count(*) as c from list1
      join list2 on list1.i = list2.i
      group by list1.i
) _
