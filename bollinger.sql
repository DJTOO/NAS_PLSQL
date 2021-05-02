--------------------------------------------------------
--  DDL for Function BOLLINGER
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."BOLLINGER" (in_symbol in varchar2,in_TRADEDATE IN DATE)
RETURN number
is
v_number number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
v_stddev number;
v_privclose1 number;
begin
select count(*) into rw_cnt from  nasdaq_avg  where symbol=in_symbol and tradedate between in_TRADEDATE-33 and in_TRADEDATE;
if (rw_cnt>=20)
then
select round(stddev(CURRENTPRICE),2) into v_stddev  from (select "SYMBOL","TRADEDATE","CURRENTPRICE",RANK() OVER (PARTITION BY symbol ORDER BY tradedate DESC) RNK from  nasdaq_avg  where symbol=in_symbol and tradedate between in_TRADEDATE-60 and in_TRADEDATE) where rnk<21;
else
v_stddev:=0;
end if;
return round(v_stddev,2);
end;

/
