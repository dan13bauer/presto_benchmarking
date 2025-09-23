--TPCH Q22
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c.c_phone from 1 for 2) as cntrycode,
            c.c_acctbal as c_acctbal
        from
            customer AS c
        where
            substring(c.c_phone from 1 for 2) in
            ('13', '31', '23', '29', '30', '18', '17')
          and c.c_acctbal > (
            select
                avg(c.c_acctbal)
            from
                customer AS c
            where
                c.c_acctbal > 0.0
              and substring(c.c_phone from 1 for 2) in
                  ('13', '31', '23', '29', '30', '18', '17')
        )
          and not exists (
            select
                *
            from
                orders AS o
            where
                o.o_custkey = c.c_custkey
        )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;
