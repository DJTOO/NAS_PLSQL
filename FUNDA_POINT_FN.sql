--------------------------------------------------------
--  DDL for Function FUNDA_POINT_FN
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."FUNDA_POINT_FN" (symbol in varchar2,ins_script IN varchar2,tradedate IN date default null)
RETURN number
is
PRAGMA AUTONOMOUS_TRANSACTION;
in_symbol varchar2(20);
in_ins_script varchar2(20);
in_tradedate date;
v_ins_date date;
v_cnt number;
v_number number;
v_ID     number;
v_curr_convert number;
v_CURRENCY varchar2(10);
v_CURRENCY3 varchar2(10);
v_revenue number;
v_avg_revenue number;
v_revenue1 number;
v_revenue2 number;
v_revenue3 number;
v_net number;
v_net1 number;
v_net2 number;
v_net3 number;
v_cf number;
v_cf1 number;
v_cf2 number;
v_cf3 number;
v_eps     number;
v_eps1     number;
v_eps2     number;
v_eps3     number;
v_prcnt2   number;
v_prcnt1   number;
v_prcnt    number;
v_point    number;
v_reportdate date;
v_reportdate1 date;
v_reportdate2 date;
v_reportdate3 date;
v_symbol varchar2(20);
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);

CURSOR YOY1 (v_SYMBOL varchar2) is  select REPORT_DATE,REVENUE,NET_INCOME,OPER_CASH_FLOW,EPS,CURRENCY from (select REPORT_DATE,SYMBOL,REVENUE,NET_INCOME,OPER_CASH_FLOW,EPS,CURRENCY,rank() over (partition by symbol order by REPORT_DATE desc) as rnk from financials ) where rnk<4 and symbol=v_SYMBOL;


begin
if (TRADEDATE is null)
 then
  select max(tradedate) into in_tradedate  from nasdaq.nasdaq_avg;
  --select current_load_date into v_tradedate1 from nse.load_date;
  else
   in_tradedate:=TRADEDATE;
end if;
in_symbol:=symbol;
in_ins_script:=ins_script;
----START CALCULATE YOY GROWTH----
v_cnt:=3;
select count(*) into rw_cnt from (select REPORT_DATE,SYMBOL,REVENUE,NET_INCOME,OPER_CASH_FLOW,EPS,rank() over (partition by symbol order by REPORT_DATE desc) as rnk from financials ) where rnk<4 and symbol=in_symbol;
DBMS_OUTPUT.put_line ('row count is   =' || rw_cnt );
open YOY1(in_symbol) ;
LOOP
FETCH YOY1 into v_reportdate,v_revenue,v_net,v_cf,v_eps,v_CURRENCY;
    EXIT WHEN YOY1%NOTFOUND;
----Fetch revenue EPS details ---

if (v_cnt=3)
then
v_CURRENCY3:=v_CURRENCY;
select value into v_curr_convert from currency_convert where currency=v_CURRENCY3;
DBMS_OUTPUT.put_line ('currency for ' || in_symbol||' is :'||v_CURRENCY3||' and conversion is '||v_curr_convert );
v_reportdate3:=v_reportdate;
v_revenue3:=nvl(trunc(v_revenue/v_curr_convert,2),0);
v_eps3:=nvl(trunc(v_eps/v_curr_convert,2),0);
v_net3:=nvl(trunc(v_net/v_curr_convert,2),0);
v_cf3:=nvl(trunc(v_cf/v_curr_convert,2),0);
elsif (v_cnt=2)
then
v_reportdate2:=v_reportdate;
v_revenue2:=nvl(trunc(v_revenue/v_curr_convert,2),0);
v_eps2:=nvl(trunc(v_eps/v_curr_convert,2),0);
v_net2:=nvl(trunc(v_net/v_curr_convert,2),0);
v_cf2:=nvl(trunc(v_cf/v_curr_convert,2),0);
elsif (v_cnt=1)
then
v_reportdate1:=v_reportdate;
v_revenue1:=nvl(trunc(v_revenue/v_curr_convert,2),0);
v_eps1:=nvl(trunc(v_eps/v_curr_convert,2),0);
v_net1:=nvl(trunc(v_net/v_curr_convert,2),0);
v_cf1:=nvl(trunc(v_cf/v_curr_convert,2),0);
else
null;
end if;
v_cnt:=v_cnt-1;
end loop;
close YOY1;

---Calculate growth in terms of percent --
DBMS_OUTPUT.put_line ('select rev3 is  = ' || v_reportdate3||'/'||v_revenue3||'/'||v_eps3 );
DBMS_OUTPUT.put_line ('select rev2 is  = ' || v_reportdate2||'/'||v_revenue2||'/'||v_eps2 );
DBMS_OUTPUT.put_line ('select rev1 is  = ' || v_reportdate1||'/'||v_revenue1||'/'||v_eps1 );
if ((v_revenue2>0) and (v_revenue1>0))
then
v_prcnt2:=trunc(((v_revenue3-v_revenue2)/v_revenue2)*100,2);
v_prcnt1:=trunc(((v_revenue2-v_revenue1)/v_revenue1)*100,2);
v_prcnt:=trunc((v_prcnt1+v_prcnt2)/2);
DBMS_OUTPUT.put_line ('both value prcnt  =' || v_prcnt );
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='YOY';
elsif (v_revenue1 is  null)
then
v_prcnt2:=trunc(((v_revenue3-v_revenue2)/v_revenue2)*100,2);
v_prcnt:=trunc((v_prcnt2)/2);
DBMS_OUTPUT.put_line ('single value prcnt  =' || v_prcnt );
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='YOY';
else --if both  v_revenue2 and v_revenue1 are null it does not fall under this category so using else instread of else if  ---
null;
end if;
v_ins_date:=sysdate;
insert into  Funda_points_log (ID,SYMBOL,INS_SCRIPT,TRADEDATE,INS_DATE,POINTS_CONFIG_ID,YOY) values  (Funda_points_seq.nextval,in_symbol,in_ins_script,in_tradedate,sysdate,v_ID,v_point);
commit;
----END YOY GROWTH ----
----START REV ZONE ---
IF (v_CURRENCY3='USD') THEN 
update  Funda_points_log set REV_ZONE=1 where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
END IF;
----END REV ZONE   ----
----START REV LEVEL ----
v_point:=0;
v_avg_revenue:=(v_revenue3+v_revenue2)/2;
select ID,VALUE into v_ID,v_point from points_config where v_avg_revenue between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='REV_LVL';
update  Funda_points_log set REV_LVL=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;

----END REV LEVEL ------

----START NET INCOME ---
DBMS_OUTPUT.put_line ('net income 3 = ' ||v_net3||' net 2 := '||v_net2||' net1 := '||v_net1 );
v_point:=0;
if (v_net3>0 and v_net2>0) then
v_point:=v_point+1;
end if;
if (v_net2<v_net3) then
v_point:=v_point+1;
end if;
if (v_net3>0 and  v_net2 is null) then
v_point:=v_point+1;
else
v_point:=v_point+0;
end if;
update  Funda_points_log set NET_INCOME=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
----END NET INCOME ---
----START NET INCOME LEVEL----
v_point:=0;
select ID,VALUE into v_ID,v_point from points_config where v_net3 between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='NET_INCOME_LVL';
update  Funda_points_log set NET_INCOME_LVL=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
----END NET INCOME LEVEL----
----START OCF ---
DBMS_OUTPUT.put_line ('CASH FLOW 3 = ' ||v_cf3||' cf 2:= '||v_cf2||' cf 1 := '||v_cf1 );
v_point:=0;
if (v_cf3>0 and v_cf2>0) then
v_point:=v_point+1;
end if;
if (v_cf2<v_cf3) then
v_point:=v_point+1;
end if;
if (v_cf3>0 and  v_cf2 is null) then
v_point:=v_point+1;
else
v_point:=v_point+0;
end if;
--DBMS_OUTPUT.put_line ('last line = ' ||in_symbol||'---'||in_ins_script||'----'||v_ins_date);
update  Funda_points_log set CASH_FLOW=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
----END OCF ---
----START OCF LVL ----
v_point:=0;
select ID,VALUE into v_ID,v_point from points_config where v_cf3 between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='CF_LVL';
update  Funda_points_log set CASH_FLOW_LVL=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;

----END OCF LVL ---
select nvl(YOY,0)+nvl(CASH_FLOW_LVL,0)+nvl(NET_INCOME_LVL,0)+nvl(NET_INCOME,0)+nvl(REV_ZONE,0)+nvl(CASH_FLOW,0)+nvl(REV_LVL,0) into v_point from Funda_points_log where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date ;
--DBMS_OUTPUT.put_line ('points = ' ||v_point);
return round(v_point,2);
end;

/
