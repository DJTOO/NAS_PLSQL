--------------------------------------------------------
--  DDL for Function NASAVG
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."NASAVG" (in_avgnum in number,in_symbol in varchar2,in_TRADEDATE IN DATE,in_parm varchar2)
RETURN number
is
v_number number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
begin
query1:='select count(*)  FROM (select symbol,TRADEDATE,'||in_parm||' from nasdaq.nasdaq_hist WHERE SYMBOL='''||in_symbol||''' AND TRADEDATE<=to_date('''||in_TRADEDATE||''',''DD-MON-YY'') ORDER BY TRADEDATE DESC) where rownum<='||in_avgnum;
--DBMS_OUTPUT.PUT_LINE('Query1: ' || query1);
EXECUTE IMMEDIATE query1 into rw_cnt ;
query2:='select sum('||in_parm||')/'||rw_cnt||'  FROM (select symbol,TRADEDATE,'||in_parm||' from nasdaq.nasdaq_hist WHERE SYMBOL='''||in_symbol||''' AND TRADEDATE<=to_date('''||in_TRADEDATE||''',''DD-MON-YY'') ORDER BY TRADEDATE DESC) where rownum<='||in_avgnum||' group by symbol order by SYMBOL';
--DBMS_OUTPUT.PUT_LINE('Query2: ' || query2);
if (rw_cnt=0)
then
null;
else
EXECUTE IMMEDIATE query2 into v_number ;
end if;
return trunc(v_number,2);
end;

/
