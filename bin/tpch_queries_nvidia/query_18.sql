--TPCH Q18
select
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    sum(l.l_quantity)
from
    customer AS c,
    orders AS o,
    lineitem AS l
where
    o.o_orderkey in (
        select
            l.l_orderkey
        from
            lineitem AS l
        group by
            l.l_orderkey having
            sum(l.l_quantity) > 314
    )
  and c.c_custkey = o.o_custkey
  and o.o_orderkey = l.l_orderkey
group by
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
order by
    o.o_totalprice desc,
    o.o_orderdate
    limit 100;
