--------------------------------------------------------
--  DDL for Procedure ANALYZE_150_PROC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "NASDAQ"."ANALYZE_150_PROC" is
  vsymbol varchar2(30);
  vgain   number;
  vloss   number;
  vdate   date;
  vcount  number;
  vrsiu   number;
  vrsid   number;
  v_avgu  number;
  v_avgd  number;
  v_avgd_adj number;
  v_rs    number;
  v_1min_price number;
  v_2min_price number;
  v_3min_price number;
  v_1max_price number;
  v_2max_price number;
  v_3max_price number;
  v_1min_date date;
  v_2min_date date;
  v_3min_date date;
  v_1max_date date;
  v_2max_date date;
  v_3max_date date;
  v_funda_points number;
  v_price_points number;
  CURSOR rev_cur IS select symbol from Down_150_log where  tradedate in (select max(tradedate) from nasdaq_avg)   and CATEGORY='R' ;
  CURSOR nonrev_cur IS select symbol from Down_150_log where  tradedate in (select max(tradedate) from nasdaq_avg)  and CATEGORY='NR' ;
BEGIN
  update Down_150_log set CATEGORY='R' where SREVENUE>20 and REVENUE>20 and TRADEDATE=(select max(tradedate) from nasdaq_avg);
  update Down_150_log set CATEGORY='NR' where ((SREVENUE is null or REVENUE is  null) or (SREVENUE<=20 or REVENUE<=20)) and TRADEDATE=(select max(tradedate) from nasdaq_avg);
  commit;
  open rev_cur;
  LOOP
  FETCH rev_cur into vsymbol;
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol);
  EXIT WHEN rev_cur%NOTFOUND;
  v_funda_points:=Funda_point_fn(symbol=>vsymbol,ins_script=>'R150');
  v_price_points:=Price_point_fn(symbol=>vsymbol,ins_script=>'R150-Price');
  select x.c.val_1, x.c.val_2 into v_1min_price,v_1min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>1) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_2min_price,v_2min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>2) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_3min_price,v_3min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>3) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_1max_price,v_1max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>1) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_2max_price,v_2max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>2) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_3max_price,v_3max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>3) c from dual ) x;
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol ||' Vaue is : '||v_funda_points||' price point:='||v_price_points);
  update Down_150_log set FUNDA_POINTS=v_funda_points,PRICE_POINTS=v_price_points,MN3=v_3min_price,MN2=v_2min_price,MN1=v_1min_price,MX3=v_3max_price,MX2=v_2max_price,MX1=v_1max_price where symbol=vsymbol and TRADEDATE=(select max(tradedate) from nasdaq_avg);
  commit;
  END LOOP;
  close rev_cur;
END;

/
