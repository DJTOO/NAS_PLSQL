--------------------------------------------------------
--  DDL for Function NAS_PRIVCLOSE
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."NAS_PRIVCLOSE" (in_symbol in varchar2,in_TRADEDATE IN DATE)
RETURN number
is
v_number number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
v_privclose number;
v_privclose1 number;
begin
select count(*) into rw_cnt from  nasdaq_hist  where symbol=in_symbol and tradedate between in_TRADEDATE-20 and in_TRADEDATE;
if (rw_cnt>=2)
then
select close into v_privclose from (select "SYMBOL","TRADEDATE","CLOSE",RANK() OVER (PARTITION BY symbol ORDER BY tradedate DESC) RNK from  nasdaq_hist  where symbol=in_symbol and tradedate between in_TRADEDATE-20 and in_TRADEDATE ) where rnk=2;
else
select close into v_privclose from (select "SYMBOL","TRADEDATE","CLOSE",RANK() OVER (PARTITION BY symbol ORDER BY tradedate DESC) RNK from  nasdaq_hist  where symbol=in_symbol and tradedate between in_TRADEDATE-20 and in_TRADEDATE ) where rnk=1;
end if;
return trunc(v_privclose,5);
end;

/
