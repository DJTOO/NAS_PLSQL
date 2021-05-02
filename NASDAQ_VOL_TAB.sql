--------------------------------------------------------
--  DDL for Function NASDAQ_VOL_TAB
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."NASDAQ_VOL_TAB" (in_symbol in varchar2,in_INS_CODE  in char)
RETURN varchar2
is
v_number number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
v_PRCNT_CHANGE number;
v_TOTDIFFVOLPRCNT number;
v_FFVOLDIFFPRCNT number;
v_privclose number;
v_privclose1 number;
v_PLUS_COUNT number;
v_LOW_COUNT number;
v_HIGN_LOW number;
v_NEG_COUNT number;
v_NEG_POSITIVE number;
v_NEG_LOWPOS_COUNT number;
v_POS_SUM NUMBER;
v_NEG_SUM NUMBER;
v_TAB varchar2(60);
v_INS_CODE char(30);
v_SYMBOL varchar2(60);
v_cnt number;
v_lng number;
v_cnt1 number;

CURSOR C1 (v_INS_CODE varchar2,v_SYMBOL varchar2) is select PRCNT_CHANGE,FFVOLDIFFPRCNT,TOTDIFFVOLPRCNT from VOL_VIEW WHERE INSER_SCRIPT=v_INS_CODE and symbol=v_SYMBOL ORDER BY PRCNT_CHANGE DESC ;

begin
v_INS_CODE:=in_INS_CODE;
v_SYMBOL:=in_symbol;
v_PLUS_COUNT:=0;
v_LOW_COUNT:=0;
v_HIGN_LOW:=0;
v_NEG_COUNT:=0;
v_NEG_POSITIVE:=0;
v_NEG_LOWPOS_COUNT:=0;
v_POS_SUM:=0;
v_NEG_SUM:=0;
--DBMS_OUTPUT.PUT_LINE('ins code is : '||v_INS_CODE);
--DBMS_OUTPUT.PUT_LINE('symbol is : '||v_SYMBOL);
--select length(trim(v_INS_CODE)) into v_lng from dual;
--DBMS_OUTPUT.PUT_LINE('v_INS_CODE length is : '||v_lng);

--select count(*) into v_cnt from vol_log where  INSER_SCRIPT='nasbull_vol_516' and symbol=v_SYMBOL ;
--select count(*) into v_cnt1 from vol_log where  INSER_SCRIPT in (v_INS_CODE);
--DBMS_OUTPUT.PUT_LINE('count is : '||v_cnt);
--DBMS_OUTPUT.PUT_LINE('count1 is : '||v_cnt1);

open C1(v_INS_CODE,v_SYMBOL) ;
LOOP
FETCH C1 into v_PRCNT_CHANGE,v_FFVOLDIFFPRCNT,v_TOTDIFFVOLPRCNT;
 EXIT WHEN C1%NOTFOUND;
-- DBMS_OUTPUT.PUT_LINE('vol diff is: '||v_FFVOLDIFFPRCNT);
-- DBMS_OUTPUT.PUT_LINE('total sum is: '||v_TOTDIFFVOLPRCNT);
if (v_PRCNT_CHANGE<0) then
v_NEG_COUNT:=v_NEG_COUNT+1;
v_NEG_SUM:=v_NEG_SUM+v_FFVOLDIFFPRCNT;
elsif (v_PRCNT_CHANGE>0) then
v_PLUS_COUNT:=v_PLUS_COUNT+1;
else
null;
end if;
end loop;
DBMS_OUTPUT.PUT_LINE('neg sum is: '||v_NEG_SUM);
DBMS_OUTPUT.PUT_LINE('Total diff is : '||v_TOTDIFFVOLPRCNT);
DBMS_OUTPUT.PUT_LINE('40% diff is : '||trunc(v_TOTDIFFVOLPRCNT)/2.5);
if (v_PLUS_COUNT=3 ) then
v_TAB:='pos_vol';
elsif (v_NEG_COUNT=3) then
v_TAB:='neg_vol';
else
if (trunc(v_TOTDIFFVOLPRCNT)/10>=abs(trunc(v_NEG_SUM))) then
v_TAB:='pos_vol';
elsif (trunc(v_TOTDIFFVOLPRCNT)/2.5<abs(trunc(v_NEG_SUM))) then
v_TAB:='neg_vol';
else
v_TAB:='dummy';
end if;
end if;
close C1;
return v_TAB;
end;

/
