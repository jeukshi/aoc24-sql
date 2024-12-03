drop materialized view if exists instructions;
drop table if exists day03_input;

create table day03_input (i text);

insert into day03_input (i) values
...

-- | Part I
select sum(m[1]::bigint * m[2]::bigint) as part1 from (
select regexp_matches(i, 'mul\((\d{1,3}),(\d{1,3})\)', 'g') m from day03_input
)_

create materialized view instructions as
select row_number() over () as ix
     , pos
     , row_number() over (partition by pos) as pos_ix
     , matches[1] ins
  from (
select
    pos,
    regexp_matches(i, 'don\''t\(\)|do\(\)|mul\(\d{1,3},\d{1,3}\)', 'g') AS matches
from (select row_number() over () as pos, i from day03_input)_
)_2

-- | Part II
with recursive gogo AS (
    select
        ix,
        pos,
        pos_ix,
        ins,
        case ins when 'do()' then 'Y'
                 when 'don''t()' then 'N'
                 else 'Y' end inc
     from instructions
    where ix = (SELECT MIN(ix) FROM instructions)
    union all
    select
        i.ix,
        i.pos,
        i.pos_ix,
        i.ins,
        case i.ins when 'do()' then 'Y'
                   when 'don''t()' then 'N'
                   else g.inc end inc
    from gogo g
    join instructions i on g.ix + 1 = i.ix
)
select sum(ins[1]::bigint * ins[2]::bigint) as part2 from (
    select regexp_matches(ins, 'mul\((\d{1,3}),(\d{1,3})\)') ins
      from gogo
     where inc = 'Y' and ins <> 'do()'
)_
