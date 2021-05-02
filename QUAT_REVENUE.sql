--------------------------------------------------------
--  DDL for Function QUAT_REVENUE
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."QUAT_REVENUE" (in_symbol in varchar2)
RETURN func_ret_three
is
v_number number;
rw_cnt number;
v_REVENUE number;
v_REPORT_DATE date;
v_RNK number;
v_REVENUE_4 NUMBER;
v_REVENUE_3 NUMBER;
v_REVENUE_2 NUMBER;
v_REVENUE_1 NUMBER;
v_PRCNT_3 NUMBER;
v_PRCNT_2 NUMBER;
v_PRCNT_1 NUMBER;
v_NEG_SUM NUMBER;
v_COUNT number;
v_TAB varchar2(60);
v_INS_CODE varchar2(6);
v_SYMBOL varchar2(60);

CURSOR C1 (v_SYMBOL varchar2) is  select DECODE(nvl(REVENUE,1),0,1,nvl(REVENUE,1)),REPORT_QUATER,rnk from (select symbol,REVENUE,REPORT_QUATER,rank() over (partition by symbol order by REPORT_QUATER desc) as rnk from QUICKFS_QUATERLY  WHERE symbol=v_SYMBOL)   where  rnk<5    order by rnk desc;

begin
v_SYMBOL:=in_symbol;
v_COUNT:=0;
v_PRCNT_2:=0;
v_PRCNT_1:=0;


open C1(v_SYMBOL) ;
LOOP
FETCH C1 into v_REVENUE,v_REPORT_DATE,v_RNK ;
    EXIT WHEN C1%NOTFOUND;
v_COUNT:=v_COUNT+1;
if (v_COUNT=1) then
v_REVENUE_4:=v_REVENUE;
elsif (v_COUNT=2) then
v_REVENUE_3:=v_REVENUE;
elsif (v_COUNT=3) then
v_REVENUE_2:=v_REVENUE;
elsif (v_COUNT=4) then
v_REVENUE_1:=v_REVENUE;
else
null;
end if;
end loop;
close C1;
v_PRCNT_3:=TRUNC(((v_REVENUE_3-v_REVENUE_4)/v_REVENUE_4),2)*100;
v_PRCNT_2:=TRUNC(((v_REVENUE_2-v_REVENUE_3)/v_REVENUE_3),2)*100;
v_PRCNT_1:=TRUNC(((v_REVENUE_1-v_REVENUE_2)/v_REVENUE_2),2)*100;
return func_ret_three(v_PRCNT_3,v_PRCNT_2,v_PRCNT_1);
end;

/
