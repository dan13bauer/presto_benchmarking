--TPCH Q20
select
    s.s_name,
    s.s_address
from
    supplier AS s,
    nation AS n
where
    s.s_suppkey in (
        select
            ps.ps_suppkey
        from
            partsupp AS ps
        where
            ps.ps_partkey in (
                select
                    p.p_partkey
                from
                    part AS p
                where
                    p.p_name like 'forest%'
            )
          and ps.ps_availqty > (
            select
                0.5 * sum(l.l_quantity)
            from
                lineitem AS l
            where
                l.l_partkey = ps.ps_partkey
              and l.l_suppkey = ps.ps_suppkey
              and l.l_shipdate >= date '1994-01-01'
              and l.l_shipdate < date '1994-01-01' + interval '1' year
        )
    )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'CANADA'
order by
    s.s_name;
