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
-- multiplication depends on the scale factor.
-- sf-1    =>  0.0001
-- sf-10   => 0.00001
-- sf-100  => 0.000001
-- sf-1000 => 0.0000001
    sum(ps.ps_supplycost * ps.ps_availqty) * 0.000001
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

