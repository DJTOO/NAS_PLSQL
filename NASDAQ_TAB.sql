--------------------------------------------------------
--  DDL for Function NASDAQ_TAB
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."NASDAQ_TAB" (in_symbol in varchar2,in_INS_CODE  in varchar2)
RETURN varchar2
is
v_number number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
v_PRCNT_CHANGE number;
v_TOTPRICEPRCNT number;
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
v_INS_CODE varchar2(6);
v_SYMBOL varchar2(60);

CURSOR C1 (v_INS_CODE varchar2,v_SYMBOL varchar2) is select PRCNT_CHANGE,TOTPRICEPRCNT from PRICE_INTERIM WHERE INS_CODE=in_INS_CODE and symbol=v_SYMBOL ORDER BY PRCNT_CHANGE DESC ;

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

open C1(v_INS_CODE,v_SYMBOL) ;
LOOP
FETCH C1 into v_PRCNT_CHANGE,v_TOTPRICEPRCNT;
    EXIT WHEN C1%NOTFOUND;
if (v_TOTPRICEPRCNT>1.5) then
if (v_PRCNT_CHANGE>0  ) then
v_PLUS_COUNT:=v_PLUS_COUNT+1;
v_POS_SUM:=v_POS_SUM+v_PRCNT_CHANGE;
elsif (v_PRCNT_CHANGE<=0 ) then
v_LOW_COUNT:=v_LOW_COUNT+1;
v_NEG_SUM:=v_NEG_SUM+v_PRCNT_CHANGE;
else
null;
end if;
elsif (v_TOTPRICEPRCNT<=0) then
if (v_PRCNT_CHANGE<0 ) then
v_NEG_COUNT:=v_NEG_COUNT+1;
v_NEG_SUM:=v_NEG_SUM+v_PRCNT_CHANGE;
elsif (v_PRCNT_CHANGE>0 ) then
v_NEG_LOWPOS_COUNT:=v_NEG_LOWPOS_COUNT+1;
v_POS_SUM:=v_POS_SUM+v_PRCNT_CHANGE;
else
null;
end if;
else
null;
end if;
end loop;
if (v_PLUS_COUNT=3 ) then
v_TAB:='all_positive';
elsif (v_PLUS_COUNT=2 and v_LOW_COUNT=1) then
if (trunc(v_POS_SUM*3)/25>=abs(trunc(v_NEG_SUM))) then --12 % OF v_POS_SUM  
v_TAB:='all_positive';
else
v_TAB:='strong_positive';
end if;
elsif (v_PLUS_COUNT=1 and v_LOW_COUNT=2) then
if (trunc(v_POS_SUM)/20>=abs(trunc(v_NEG_SUM))) then
v_TAB:='strong_positive';
else
v_TAB:='dummy';
end if;
elsif (v_NEG_COUNT=3 or (v_NEG_COUNT=2 and v_NEG_LOWPOS_COUNT=1 )) then
v_TAB:='all_negitive';
elsif (v_NEG_COUNT=2 and v_NEG_POSITIVE=1) then
v_TAB:='strong_negitive';
else
v_TAB:='dummy';
end if;
close C1;
return v_TAB;
end;

/
