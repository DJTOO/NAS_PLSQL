--------------------------------------------------------
--  DDL for Function QOQ_REV
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."QOQ_REV" (in_symbol in varchar2)
RETURN func_ret_four
is

rw_cnt number;
v_EPS number;
v_REPORT_QUATER date;
v_RNK number;
v_REV number;
v_CURR_REV_4 NUMBER;
v_CURR_REV_3 NUMBER;
v_CURR_REV_2 NUMBER;
v_CURR_REV_1 NUMBER;
v_PRE_REV_4 NUMBER;
v_PRE_REV_3 NUMBER;
v_PRE_REV_2 NUMBER;
v_PRE_REV_1 NUMBER;
v_PRCNT_4 NUMBER;
v_PRCNT_3 NUMBER;
v_PRCNT_2 NUMBER;
v_PRCNT_1 NUMBER;
v_NEG_CNT NUMBER;
v_COUNT number;
v_SYMBOL varchar2(60);


CURSOR C1 (v_SYMBOL varchar2) is  select REVENUE,REPORT_QUATER,rank() over (partition by symbol order by REPORT_QUATER ASC ) as rnk from QUICKFS_QUATERLY  WHERE symbol=v_SYMBOL AND TO_CHAR(REPORT_QUATER,'YYYY')=(SELECT TO_CHAR(SYSDATE-60,'YYYY') FROM DUAL);

CURSOR C2 (v_SYMBOL varchar2) is  select DECODE(nvl(REVENUE,1),0,.01,nvl(REVENUE,1)),REPORT_QUATER,rank() over (partition by symbol order by REPORT_QUATER ASC ) as rnk from QUICKFS_QUATERLY  WHERE symbol=v_SYMBOL AND TO_CHAR(REPORT_QUATER,'YYYY')=(SELECT TO_CHAR(SYSDATE-60,'YYYY')-1 FROM DUAL);


begin
v_SYMBOL:=in_symbol;
v_COUNT:=0;
v_PRCNT_2:=0;
v_PRCNT_1:=0;
open C1(v_SYMBOL) ;
LOOP
FETCH C1 into v_REV,v_REPORT_QUATER,v_RNK ;
    EXIT WHEN C1%NOTFOUND;
v_COUNT:=v_COUNT+1;
if (v_RNK=1) then
v_CURR_REV_1:=v_REV;
elsif (v_RNK=2) then
v_CURR_REV_2:=v_REV;
elsif (v_RNK=3) then
v_CURR_REV_3:=v_REV;
elsif (v_RNK=4) then
v_CURR_REV_4:=v_REV;
else
null;
end if;
end loop;
close C1;
open C2(v_SYMBOL) ;
LOOP
FETCH C2 into v_REV,v_REPORT_QUATER,v_RNK ;
    EXIT WHEN C2%NOTFOUND;
v_COUNT:=v_COUNT+1;
if (v_RNK=1) then
v_PRE_REV_1:=v_REV;
elsif (v_RNK=2) then
v_PRE_REV_2:=v_REV;
elsif (v_RNK=3) then
v_PRE_REV_3:=v_REV;
elsif (v_RNK=4) then
v_PRE_REV_4:=v_REV;
else
null;
end if;
end loop;
close C2;
if ((v_CURR_REV_4 IS NOT NULL))  then
v_PRCNT_4:=TRUNC(((v_CURR_REV_4-v_PRE_REV_4)/v_PRE_REV_4),2)*100;
end if;
if ((v_CURR_REV_3 IS NOT NULL))  then
v_PRCNT_3:=TRUNC(((v_CURR_REV_3-v_PRE_REV_3)/v_PRE_REV_3),2)*100;
end if;
if ((v_CURR_REV_2 IS NOT NULL))  then
v_PRCNT_2:=TRUNC(((v_CURR_REV_2-v_PRE_REV_2)/v_PRE_REV_2),2)*100;
end if;
if ((v_CURR_REV_1 IS NOT NULL))  then
v_PRCNT_1:=TRUNC(((v_CURR_REV_1-v_PRE_REV_1)/v_PRE_REV_1),2)*100;
end if;
return func_ret_four(v_PRCNT_4,v_PRCNT_3,v_PRCNT_2,v_PRCNT_1);
end;

/
