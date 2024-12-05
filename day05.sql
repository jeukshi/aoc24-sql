drop materialized view if exists page_result;
drop materialized view if exists page_middle_value;
drop materialized view if exists pages;
drop materialized view if exists ordering_rules;
drop table if exists day05_input;

create table day05_input ( i text);

insert into day05_input (i) values
...

create materialized view ordering_rules as
select row_number() over () ordering_rule_id
     , split_part(i, '|', 1)::bigint fst
     , split_part(i, '|', 2)::bigint snd
  from day05_input
 where i like '%|%'

create materialized view pages as
select *
     , coalesce(array_agg(val)
         over (partition by page_id order by val_id
               rows between unbounded preceding and 1 preceding)
               , '{}'
       ) as vals_before
  from (
select page_id
     , row_number() over (partition by page_id) val_id
     , val::bigint
    from (
    select page_id, string_to_table(i, ',') val
      from (
        select row_number() over() page_id, *
          from day05_input
         where i like '%,%'
    )_
)_2
)_3

create materialized view page_middle_value as
select page_id m_page_id, val m_val from (
    select *
         , max(val_id) over (partition by page_id) max_val_id
      from pages
)
where val_id = ((max_val_id + 1) / 2)

create materialized view page_result as
select page_id, bool_or(rule_violated) rules_violated
  from (
select *
     , coalesce(snd = any(vals_before), false) as rule_violated
  from pages
  left join ordering_rules on fst = val
)_
group by page_id

-- | Part I
select sum(m_val) as part1
  from page_result
  join page_middle_value on page_id = m_page_id
 where rules_violated = false

select * from (
select *
     , coalesce(snd = any(vals_before), false) as rule_violated
  from pages
  left join ordering_rules on fst = val
) where rule_violated = true
and page_id = 6
order by val_id

-- | Part II, could use some indexes
with recursive gogo as (
select 0 as page_ver
	 , pages.page_id
     , array_agg(val) vals
  from pages
  join page_result on pages.page_id = page_result.page_id
 where rules_violated = true
 group by pages.page_id
union
select page_ver + 1 as page_ver
     , page_id
    , case
        when fst_pos = 1 then vals[2:array_length(vals, 1)]
        when fst_pos = array_length(vals, 1) - 1 then vals[1:array_length(vals, 1) - 1]
        else array_cat(vals[1:fst_pos], vals[fst_pos+2:array_length(vals, 1)])
       end as vals
  from (
    select distinct on (page_id)
           page_ver
         , page_id
         , fst_pos
         , snd_pos
         , case when snd_pos = 1
                then array_cat(array[fst], vals[1:array_length(vals, 1)])
                else array_cat(
                          array_cat( vals[1:(snd_pos - 1)], array[fst] )
                        , vals[snd_pos:array_length(vals, 1)]) end as vals
      from (
        select *
             , array_position(vals, fst) fst_pos
             , array_position(vals, snd) snd_pos
          from (
            select page_ver, page_id, vals
              from gogo
        )_ join ordering_rules on true
    )_2
    where fst_pos > snd_pos
    order by page_id, vals, ordering_rule_id
)_3
)
select sum(vals[(array_length(vals, 1)+1)/2]) part2
  from (
    select distinct on (page_id)
           page_ver
         , page_id
         , vals
      from gogo
      order by page_id, page_ver desc
)
