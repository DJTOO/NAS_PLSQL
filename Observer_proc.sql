create or replace procedure   observer_proc  is
  vsymbol varchar2(30);
  v_ins_price number;
  vgain   number;
  vloss   number;
  vdate   date;
  v_cnt    number;
  vcount  number;
  vrsiu   number;
  vrsid   number;
  v_avgu  number;
  v_avgd  number;
  v_avgd_adj number;
  v_rs    number;
  v_funda_points number;
  v_price_points number;
  v_CURRENTPRICE number;
  v_TRADEDATE    date;
  v_PRCNT_CHANGE number;
  v_FIVEDAYAVGPRI number;
  v_BU number;
  v_TWENTYAVGPRI number;
  v_BD number;
  v_FIFTYAVGPRI number;
  v_ONEFIFTYAVGPRI number;
  v_TWOHUNAVGPRI number;
  v_CURRENTVOL number;
  v_FIVEDAYVOL number;
  v_TWENTYAVGVOL number;
  v_RSI number;
  v_SREVENUE number;
  v_REVENUE number;
  v_SEPS number;
  v_EPS number;
  
  CURSOR observe_cur IS select symbol,INS_PRICE from nasdaq.observer_log ;
  BEGIN
  insert into nasdaq.observer_log (SYMBOL,INS_DATE,INS_PRICE,INS_FUNDA_POINTS,INS_PRICE_POINTS,CATEGORY,SUBCATEGORY) select SYMBOL,TRADEDATE,CURRENTPRICE,FUNDA_POINTS,PRICE_POINTS,CATEGORY,'FUND+PRICE' from nasdaq.Down_150_log where CATEGORY='R' and (FUNDA_POINTS+PRICE_POINTS/2)>16 and symbol not in (select symbol from  nasdaq.observer_log) and tradedate=(select max(tradedate) from nasdaq_avg) ;
  insert into nasdaq.observer_log (SYMBOL,INS_DATE,INS_PRICE,INS_FUNDA_POINTS,INS_PRICE_POINTS,CATEGORY,SUBCATEGORY) select SYMBOL,TRADEDATE,CURRENTPRICE,FUNDA_POINTS,PRICE_POINTS,CATEGORY,'FUND-FUNDA' from nasdaq.Down_150_log where CATEGORY='R' and (FUNDA_POINTS)>13 and symbol not in (select symbol from  nasdaq.observer_log)  and tradedate=(select max(tradedate) from nasdaq_avg);
  commit;
  open observe_cur;
  LOOP
  FETCH observe_cur into vsymbol,v_ins_price;
  EXIT WHEN observe_cur%NOTFOUND;
  select count(*) into v_cnt from nasdaq.nasdaq_avg where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol||'cnt is '||v_cnt);
  if (v_cnt=1) then
  select CURRENTPRICE,TRADEDATE,PRCNT_CHANGE,FIVEDAYAVGPRI,BU,TWENTYAVGPRI,BD,FIFTYAVGPRI,ONEFIFTYAVGPRI,TWOHUNAVGPRI,CURRENTVOL,FIVEDAYVOL,TWENTYAVGVOL,RSI into v_CURRENTPRICE,v_TRADEDATE,v_PRCNT_CHANGE,v_FIVEDAYAVGPRI,v_BU,v_TWENTYAVGPRI,v_BD,v_FIFTYAVGPRI,v_ONEFIFTYAVGPRI,v_TWOHUNAVGPRI,v_CURRENTVOL,v_FIVEDAYVOL,v_TWENTYAVGVOL,v_RSI from nasdaq.nasdaq_avg where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  select SREVENUE,REVENUE,SEPS,EPS into v_SREVENUE,v_REVENUE,v_SEPS,v_EPS from  fundav where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  update nasdaq.observer_log set CURRENTPRICE=v_CURRENTPRICE,TRADEDATE=v_TRADEDATE,SREVENUE=v_SREVENUE,REVENUE=v_REVENUE,SEPS=v_SEPS,EPS=v_EPS,PRCNT_CHANGE=v_PRCNT_CHANGE,PROFIT_LOSS=trunc(((v_CURRENTPRICE-v_ins_price)/v_ins_price)*100,2),FIVEDAYAVGPRI=v_FIVEDAYAVGPRI,BU=v_BU,TWENTYAVGPRI=v_TWENTYAVGPRI,BD=v_BD,FIFTYAVGPRI=v_FIFTYAVGPRI,ONEFIFTYAVGPRI=v_ONEFIFTYAVGPRI,TWOHUNAVGPRI=v_TWOHUNAVGPRI,CURRENTVOL=v_CURRENTVOL,FIVEDAYVOL=v_FIVEDAYVOL,TWENTYAVGVOL=v_TWENTYAVGVOL,RSI=v_RSI where symbol=vsymbol;
  commit;
  end if;
  END LOOP;
  close observe_cur;
END;