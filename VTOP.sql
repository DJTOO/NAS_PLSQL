--------------------------------------------------------
--  DDL for Function VTOP
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."VTOP" (in_symbol in varchar2,in_INSERTDATE IN DATE)
RETURN number
is
v_number number;
v_cnt number;
v_cnt_hist number;
rw_cnt number;
v_FIVEDAYAVGPRI number;
v_PRCNT_CHANGE  number;
v_FIFTYAVGVOL number;
v_VOLPRCNT number;
v_tradedate_hist date;
query2 varchar2(1000);
v_repeat number;
v_sum number;
begin
v_sum:=0;
v_cnt:=0;
select count(*) into v_cnt from vol_toppers_hist where tradedate=in_INSERTDATE and symbol=in_symbol;
select count(*) into v_cnt_hist from vol_toppers_hist where tradedate>sysdate-30 and symbol=in_symbol;
if (v_cnt>0)
then
select VOLPRCNT,FIFTYAVGVOL,PRCNT_CHANGE into v_VOLPRCNT,v_FIFTYAVGVOL,v_PRCNT_CHANGE from vol_toppers_hist where tradedate=in_INSERTDATE and symbol=in_symbol;
v_sum:=trunc(v_VOLPRCNT/100);
DBMS_OUTPUT.PUT_LINE('VOLPRCNT SUM is   '||v_sum);
--Decide percent growth on volume avg ass its to double in lower volumes
if (v_FIFTYAVGVOL>=0  and v_FIFTYAVGVOL<=1)
then
v_sum:=v_sum-2;
elsif (v_FIFTYAVGVOL>1  and v_FIFTYAVGVOL<=5)
then
v_sum:=v_sum-1;
elsif (v_FIFTYAVGVOL>5  and v_FIFTYAVGVOL<=16)
then
v_sum:=v_sum+1;
elsif (v_FIFTYAVGVOL>16  and v_FIFTYAVGVOL<=25)
then
v_sum:=v_sum+1;
elsif (v_FIFTYAVGVOL>25  and v_FIFTYAVGVOL<=75)
then
v_sum:=v_sum+2;
elsif (v_FIFTYAVGVOL>75 )
then
v_sum:=v_sum+2;
else
    null;
end if;
DBMS_OUTPUT.PUT_LINE('FIFTYAVGVOL SUM is   '||v_sum);
--Decide percent growth on day of volume
if (v_PRCNT_CHANGE<0)
then
v_sum:=v_sum+2*round(v_PRCNT_CHANGE);
elsif (v_PRCNT_CHANGE>=0  and v_PRCNT_CHANGE<=3)
then
v_sum:=v_sum-2;
elsif (v_PRCNT_CHANGE>3  and v_PRCNT_CHANGE<=5)
then
v_sum:=v_sum+0;
elsif (v_PRCNT_CHANGE>5  and v_PRCNT_CHANGE<=10)
then
v_sum:=v_sum+1;
DBMS_OUTPUT.PUT_LINE('entered 5,10 SUM is   '||v_sum);
elsif (v_PRCNT_CHANGE>10)
then
v_sum:=v_sum+1;
DBMS_OUTPUT.PUT_LINE('entered 10 SUM is   '||v_sum);
else
null;
end if;
DBMS_OUTPUT.PUT_LINE('PRCNT_CHANGE SUM is   '||v_sum);
elsif (v_cnt=0 and v_cnt_hist>0)
then
    select max(tradedate) into v_tradedate_hist from vol_toppers_hist  where tradedate>sysdate-15 and symbol=in_symbol;
select VOLPRCNT,FIFTYAVGVOL,PRCNT_CHANGE into v_VOLPRCNT,v_FIFTYAVGVOL,v_PRCNT_CHANGE from vol_toppers_hist where tradedate=v_tradedate_hist and symbol=in_symbol;
v_sum:=trunc(v_VOLPRCNT/100);
v_sum:=1;
DBMS_OUTPUT.PUT_LINE('hist VOLPRCNT SUM is   '||v_sum);
--Decide percent growth on volume avg ass its to double in lower volumes
if (v_FIFTYAVGVOL>=0  and v_FIFTYAVGVOL<=1)
then
v_sum:=v_sum-2;
elsif (v_FIFTYAVGVOL>1  and v_FIFTYAVGVOL<=5)
then
v_sum:=v_sum-1;
elsif (v_FIFTYAVGVOL>5  and v_FIFTYAVGVOL<=16)
then
v_sum:=v_sum+1;
elsif (v_FIFTYAVGVOL>16  and v_FIFTYAVGVOL<=25)
then
v_sum:=v_sum+1;
elsif (v_FIFTYAVGVOL>25  and v_FIFTYAVGVOL<=75)
then
v_sum:=v_sum+2;
elsif (v_FIFTYAVGVOL>75 )
then
v_sum:=v_sum+3;
else
    null;
end if;
DBMS_OUTPUT.PUT_LINE('hist FIFTYAVGVOL SUM is   '||v_sum);
--Decide percent growth on day of volume
if (v_PRCNT_CHANGE<0)
then
v_sum:=v_sum+v_PRCNT_CHANGE;
elsif (v_PRCNT_CHANGE>=0  and v_PRCNT_CHANGE<=3)
then
v_sum:=v_sum-2;
elsif (v_PRCNT_CHANGE>3  and v_PRCNT_CHANGE<=5)
then
v_sum:=v_sum+0;
elsif (v_PRCNT_CHANGE>5  and v_PRCNT_CHANGE<=10)
then
v_sum:=v_sum+1;
elsif (v_PRCNT_CHANGE>10)
then
v_sum:=v_sum+1;
else
null;
end if;
DBMS_OUTPUT.PUT_LINE('hist PRCNT_CHANGE SUM is   '||v_sum);
else
v_sum:=0;
end if;
select count(*) into v_repeat from vol_toppers_hist where symbol=in_symbol and tradedate>sysdate-60;
DBMS_OUTPUT.PUT_LINE('REPEAT  is   '||v_repeat);
if (v_repeat>1)
then
v_sum:=v_sum+2*v_repeat;
else
null;
end if;
v_number:=v_sum;
return trunc(v_number,2);
end;

/
