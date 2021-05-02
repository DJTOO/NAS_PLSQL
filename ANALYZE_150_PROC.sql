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
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol ||' Vaue is : '||v_funda_points||' price point:='||v_price_points);
  update Down_150_log set FUNDA_POINTS=v_funda_points,PRICE_POINTS=v_price_points where symbol=vsymbol and TRADEDATE=(select max(tradedate) from nasdaq_avg);
  commit;
  END LOOP;
  close rev_cur;
END;

/
