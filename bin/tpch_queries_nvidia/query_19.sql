--TPCH Q19
select
    sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
    lineitem AS l,
    part AS p
where
    (
        p.p_partkey = l.l_partkey
            and p.p_brand = 'Brand#12'
            and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l.l_quantity >= 1 and l.l_quantity <= 1 + 10
            and p.p_size between 1 and 5
            and l.l_shipmode in ('AIR', 'AIR REG')
            and l.l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
        p.p_partkey = l.l_partkey
            and p.p_brand = 'Brand#23'
            and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l.l_quantity >= 10 and l.l_quantity <= 10 + 10
            and p.p_size between 1 and 10
            and l.l_shipmode in ('AIR', 'AIR REG')
            and l.l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
        p.p_partkey = l.l_partkey
            and p.p_brand = 'Brand#34'
            and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l.l_quantity >= 20 and l.l_quantity <= 20 + 10
            and p.p_size between 1 and 15
            and l.l_shipmode in ('AIR', 'AIR REG')
            and l.l_shipinstruct = 'DELIVER IN PERSON'
        );
