--------------------------------------------------------
--  DDL for Function YEAR_EPS
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."YEAR_EPS" (in_symbol in varchar2)
RETURN func_ret_three
is
v_number number;
rw_cnt number;
v_EPS number;
v_REPORT_DATE date;
v_RNK number;
v_EPS_3 NUMBER;
v_EPS_2 NUMBER;
v_EPS_1 NUMBER;
v_PRCNT_2 NUMBER;
v_PRCNT_1 NUMBER;
v_NEG_CNT NUMBER;
v_COUNT number;
v_TAB varchar2(60);
v_INS_CODE varchar2(6);
v_SYMBOL varchar2(60);


CURSOR C1 (v_SYMBOL varchar2) is select DECODE(nvl(EPS,.01),0,.01,nvl(EPS,.01)),REPORT_DATE,rnk from (select symbol,EPS,REPORT_DATE,CURRENCY,rank() over (partition by symbol order by REPORT_DATE desc) as rnk from financials WHERE symbol=v_SYMBOL )   where  rnk<4   order by rnk desc;

begin
v_SYMBOL:=in_symbol;
v_COUNT:=0;
v_PRCNT_2:=0;
v_PRCNT_1:=0;
open C1(v_SYMBOL) ;
LOOP
FETCH C1 into v_EPS,v_REPORT_DATE,v_RNK ;
    EXIT WHEN C1%NOTFOUND;
v_COUNT:=v_COUNT+1;
if (v_COUNT=1) then
v_EPS_3:=v_EPS;
elsif (v_COUNT=2) then
v_EPS_2:=v_EPS;
elsif (v_COUNT=3) then
v_EPS_1:=v_EPS;
else
null;
end if;
end loop;
close C1;
v_PRCNT_2:=TRUNC(((v_EPS_2-v_EPS_3)/ABS(v_EPS_3)),2)*100;
v_PRCNT_1:=TRUNC(((v_EPS_1-v_EPS_2)/ABS(v_EPS_2)),2)*100;
select COUNT(*) INTO v_NEG_CNT from (select symbol,EPS,REPORT_DATE,CURRENCY,rank() over (partition by symbol order by REPORT_DATE desc) as rnk from financials WHERE  symbol=in_symbol)   where  rnk=1  AND  EPS<=0 order by rnk desc;
return func_ret_three(v_PRCNT_2,v_PRCNT_1,v_NEG_CNT);
end;

/
