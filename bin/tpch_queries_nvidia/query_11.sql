--TPCH Q11
select
    ps.ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) as value
from
    partsupp AS ps,
    supplier AS s,
    nation AS n
where
    ps.ps_suppkey = s.s_suppkey
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'GERMANY'
group by
    ps.ps_partkey having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
    select
    sum(ps.ps_supplycost * ps.ps_availqty) * 0.0_0001
    from
    partsupp AS ps,
    supplier AS s,
    nation AS n
    where
    ps.ps_suppkey = s.s_suppkey
                  and s.s_nationkey = n.n_nationkey
                  and n.n_name = 'GERMANY'
    )
order by
    value desc;
