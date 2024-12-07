drop materialized view if exists calibration_equations;
drop table if exists day07_input;

create table day07_input ( i text);

insert into day07_input (i) values
...

create materialized view calibration_equations as
select res_id
     , res
     , row_number() over (partition by res_id) as num_id
     , num
  from (
    select *
         , string_to_table(rest, ' ')::bigint as num
      from (
          select row_number() over () res_id, res, rest
            from (
              select split_part(i, ': '::text, 1)::bigint as res
                   , split_part(i, ': '::text, 2)::text as rest
                from day07_input
        ) _
    )_2
)_3

-- | Part I
with recursive gogo as (
select res_id as go_res_id
     , res as go_res
     , num_id as go_num_id
     , num as go_op_res
  from calibration_equations
 where num_id = 1
union all
select * from ( with igo as ( select * from gogo)
select go_res_id
     , go_res
     , go_num_id + 1
     , go_op_res * num
  from igo
  join calibration_equations on  go_res_id = res_id and num_id = go_num_id + 1
union all
select go_res_id
     , go_res
     , go_num_id + 1
     , go_op_res + num
  from igo
  join calibration_equations on  go_res_id = res_id and num_id = go_num_id + 1
)
)
select sum(go_op_res) as part1 from (
    select distinct on (go_res_id) go_res_id, go_op_res
    from gogo
    join (select res_id, max(num_id) max_num_id from calibration_equations group by res_id)
      on go_res_id = res_id and go_num_id = max_num_id
   where go_res = go_op_res
   order by go_res_id
)_

-- | Part II
with recursive gogo as (
select res_id as go_res_id
     , res as go_res
     , num_id as go_num_id
     , num as go_op_res
  from calibration_equations
 where num_id = 1
union all
select * from ( with igo as ( select * from gogo)
select go_res_id
     , go_res
     , go_num_id + 1
     , go_op_res * num
  from igo
  join calibration_equations on  go_res_id = res_id and num_id = go_num_id + 1
union all
select go_res_id
     , go_res
     , go_num_id + 1
     , go_op_res + num
  from igo
  join calibration_equations on  go_res_id = res_id and num_id = go_num_id + 1
union all
select go_res_id
     , go_res
     , go_num_id + 1
     , (go_op_res::text || num::text)::bigint
  from igo
  join calibration_equations on  go_res_id = res_id and num_id = go_num_id + 1
)
)
select sum(go_op_res) as part2 from (
    select distinct on (go_res_id) go_res_id, go_op_res
    from gogo
    join (select res_id, max(num_id) max_num_id from calibration_equations group by res_id)
      on go_res_id = res_id and go_num_id = max_num_id
   where go_res = go_op_res
   order by go_res_id
)_
