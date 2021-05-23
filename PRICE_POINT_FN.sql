--------------------------------------------------------
--  DDL for Function PRICE_POINT_FN
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."PRICE_POINT_FN" (symbol in varchar2,ins_script IN varchar2,tradedate IN date default null)
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
v_CURRENTPRICE number;
v_ONEFIFTYAVGPRI number;
v_TWOHUNAVGPRI number;
v_prcnt    number;
v_prcnt2   number;
v_point    number;
v_final_point    number;
v_symbol varchar2(20);
v_1min_price number;
v_2min_price number;
v_3min_price number;
v_1min_date date;
v_2min_date date;
v_3min_date date;
v_1max_price number;
v_2max_price number;
v_3max_price number;
v_1max_date date;
v_2max_date date;
v_3max_date date;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);

--CURSOR YOY1 (v_SYMBOL varchar2) is  select SYMBOL,TRADEDATE,CURRENTPRICE,FIVEDAYAVGPRI,TWOHUNAVGPRI from nasdaq_avg where  symbol=v_SYMBOL and tradedate=(select max(tradedate) from nasdaq_avg) ;


begin
select max(tradedate) into in_tradedate from nasdaq_avg;
in_symbol:=symbol;
in_ins_script:=ins_script;
if (tradedate is null)
 then
  select max(tradedate) into in_tradedate  from nasdaq.nasdaq_avg;
  --select current_load_date into v_tradedate1 from nse.load_date;
elsif (tradedate>in_tradedate) then
   null;
else
in_tradedate:=tradedate;
end if;
v_ins_date:=sysdate;
DBMS_OUTPUT.put_line ('query date is  = '||in_tradedate||' insert date is '||v_ins_date||' symbol is '||in_symbol);
select CURRENTPRICE,nvl(ONEFIFTYAVGPRI,0),nvl(TWOHUNAVGPRI,0) into v_CURRENTPRICE,v_ONEFIFTYAVGPRI,v_TWOHUNAVGPRI from nasdaq_avg where  symbol=in_symbol and tradedate=in_tradedate ;
DBMS_OUTPUT.put_line (' current price '||v_CURRENTPRICE||' 150 avg  is  = '||v_ONEFIFTYAVGPRI||' 200 avg is '||v_TWOHUNAVGPRI);
--- we want lowest avg price to be in twohundraredavg so swapping in case if it is not --
if (v_TWOHUNAVGPRI>v_ONEFIFTYAVGPRI) then
select CURRENTPRICE,nvl(ONEFIFTYAVGPRI,0),nvl(TWOHUNAVGPRI,0) into v_CURRENTPRICE,v_TWOHUNAVGPRI,v_ONEFIFTYAVGPRI from nasdaq_avg where  symbol=in_symbol and tradedate=in_tradedate ;
end if;
----start assesing price level in comaprison to 150 200 avgs ---
v_prcnt:=trunc(((v_CURRENTPRICE-v_ONEFIFTYAVGPRI)/v_ONEFIFTYAVGPRI)*100,2);
DBMS_OUTPUT.put_line ('56 line prcnt 2  = ' ||v_prcnt);
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='PRICE_150';
DBMS_OUTPUT.put_line ('symbol = ' ||in_symbol||' script '||in_ins_script||' insdate : '||v_ins_date||' prcnt is '||v_prcnt||'points is '||v_point);
insert into  price_points_log (ID,SYMBOL,INS_SCRIPT,TRADEDATE,INS_DATE,POINTS_CONFIG_ID,LVL_150) values  (Price_points_seq.nextval,in_symbol,in_ins_script,in_tradedate,v_ins_date,v_ID,v_point);
commit;
v_prcnt2:=trunc(((v_CURRENTPRICE-v_TWOHUNAVGPRI)/v_TWOHUNAVGPRI)*100,2);
DBMS_OUTPUT.put_line ('prcnt 2  = ' ||v_prcnt2);
select ID,VALUE into v_ID,v_point from points_config where v_prcnt2 between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='PRICE_200';
DBMS_OUTPUT.put_line ('200 lvl points = ' ||v_point);
DBMS_OUTPUT.put_line ('symbol = ' ||in_symbol||' script '||in_ins_script||' insdate : '||v_ins_date);
update  Price_points_log set LVL_200=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
----END  assesing price level in comaprison to 150 200 avgs ---
----start assesing all time lows in price  ---
select x.c.val_1, x.c.val_2 into v_1min_price,v_1min_date from (select Min_Max_Price_fn(in_symbol=>in_symbol,Minmax=>'MIN',years=>1) c from dual ) x;
select x.c.val_1, x.c.val_2 into v_2min_price,v_2min_date from (select Min_Max_Price_fn(in_symbol=>in_symbol,Minmax=>'MIN',years=>2) c from dual ) x;
select x.c.val_1, x.c.val_2 into v_3min_price,v_3min_date from (select Min_Max_Price_fn(in_symbol=>in_symbol,Minmax=>'MIN',years=>3) c from dual ) x;
if ((1.2*v_3min_price)>=v_CURRENTPRICE) then
v_prcnt:=round(((v_CURRENTPRICE-v_3min_price)/v_3min_price)*100,2);
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='PRICE_3LOW';
update  Price_points_log set LOW3=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
elsif ((1.2*v_2min_price)>=v_CURRENTPRICE) then
v_prcnt:=round(((v_CURRENTPRICE-v_2min_price)/v_2min_price)*100,2);
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='PRICE_2LOW';
update  Price_points_log set LOW2=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
elsif ((1.2*v_1min_price)>=v_CURRENTPRICE) then
v_prcnt:=round(((v_CURRENTPRICE-v_1min_price)/v_1min_price)*100,2);
select ID,VALUE into v_ID,v_point from points_config where v_prcnt between VAL_MIN and VAL_MAX and status='Y' and PARAMETER='PRICE_1LOW';
update  Price_points_log set LOW1=v_point where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date;
commit;
else
null;
end if;
select nvl(LVL_200,0)+nvl(LVL_150,0)+nvl(LOW3,0)+nvl(LOW2,0)++nvl(LOW1,0) into v_point from Price_points_log where symbol=in_symbol and INS_SCRIPT=in_ins_script and INS_DATE=v_ins_date ;
DBMS_OUTPUT.put_line ('points = ' ||v_point);
return round(v_point,2);
end;

/
