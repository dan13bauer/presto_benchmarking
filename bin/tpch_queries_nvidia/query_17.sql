--TPCH Q17
select
    sum(l.l_extendedprice) / 7.0 as avg_yearly
from
    lineitem AS l,
    part AS p
where
    p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select
        0.2 * avg(l.l_quantity)
    from
        lineitem AS l
    where
        l.l_partkey = p.p_partkey);
