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
  v_CATEGORY  varchar2(10);
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
  v_MN3 number;
  v_MN2 number;
  v_MN1 number;
  v_MX3 number;
  v_MX2 number;
  v_MX1 number;
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
  --v_FUNDA_POINTS number;
  --v_PRICE_POINTS number;

  CURSOR observe_cur IS select symbol,INS_PRICE,CATEGORY from nasdaq.observer_log ;
  BEGIN
  insert into observer_log_log select * from observer_log;
  commit;
  ---- intake new records from down_150_log ---
  insert into nasdaq.observer_log (SYMBOL,INS_DATE,INS_PRICE,INS_FUNDA_POINTS,INS_PRICE_POINTS,CATEGORY,SUBCATEGORY,state) select SYMBOL,TRADEDATE,CURRENTPRICE,FUNDA_POINTS,PRICE_POINTS,CATEGORY,'FUND+PRICE','NEW' from nasdaq.Down_150_log where CATEGORY='R' and (FUNDA_POINTS+(3/4)*PRICE_POINTS)>18 and symbol not in (select symbol from  nasdaq.observer_log) and tradedate=(select max(tradedate) from nasdaq_avg) ;
  insert into nasdaq.observer_log (SYMBOL,INS_DATE,INS_PRICE,INS_FUNDA_POINTS,INS_PRICE_POINTS,CATEGORY,SUBCATEGORY,state) select SYMBOL,TRADEDATE,CURRENTPRICE,FUNDA_POINTS,PRICE_POINTS,CATEGORY,'FUND-FUNDA','NEW' from nasdaq.Down_150_log where CATEGORY='R' and (FUNDA_POINTS)>13 and PRICE_POINTS>2 and symbol not in (select symbol from  nasdaq.observer_log)  and tradedate=(select max(tradedate) from nasdaq_avg);
  commit;
  open observe_cur;
  LOOP
  FETCH observe_cur into vsymbol,v_ins_price,v_CATEGORY;
  EXIT WHEN observe_cur%NOTFOUND;
  select count(*) into v_cnt from nasdaq.nasdaq_avg where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol||'cnt is '||v_cnt);
  if (v_cnt=1) then
  select CURRENTPRICE,TRADEDATE,PRCNT_CHANGE,FIVEDAYAVGPRI,BU,TWENTYAVGPRI,BD,FIFTYAVGPRI,ONEFIFTYAVGPRI,TWOHUNAVGPRI,CURRENTVOL,FIVEDAYVOL,TWENTYAVGVOL,RSI into v_CURRENTPRICE,v_TRADEDATE,v_PRCNT_CHANGE,v_FIVEDAYAVGPRI,v_BU,v_TWENTYAVGPRI,v_BD,v_FIFTYAVGPRI,v_ONEFIFTYAVGPRI,v_TWOHUNAVGPRI,v_CURRENTVOL,v_FIVEDAYVOL,v_TWENTYAVGVOL,v_RSI from nasdaq.nasdaq_avg where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  select SREVENUE,REVENUE,SEPS,EPS into v_SREVENUE,v_REVENUE,v_SEPS,v_EPS from  fundav where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  update nasdaq.observer_log set CURRENTPRICE=v_CURRENTPRICE,TRADEDATE=v_TRADEDATE,SREVENUE=v_SREVENUE,REVENUE=v_REVENUE,SEPS=v_SEPS,EPS=v_EPS,PRCNT_CHANGE=v_PRCNT_CHANGE,PROFIT_LOSS=trunc(((v_CURRENTPRICE-v_ins_price)/v_ins_price)*100,2),FIVEDAYAVGPRI=v_FIVEDAYAVGPRI,BU=v_BU,TWENTYAVGPRI=v_TWENTYAVGPRI,BD=v_BD,FIFTYAVGPRI=v_FIFTYAVGPRI,ONEFIFTYAVGPRI=v_ONEFIFTYAVGPRI,TWOHUNAVGPRI=v_TWOHUNAVGPRI,CURRENTVOL=v_CURRENTVOL,FIVEDAYVOL=v_FIVEDAYVOL,TWENTYAVGVOL=v_TWENTYAVGVOL,RSI=v_RSI where symbol=vsymbol;
  update nasdaq.observer_log set state='CONTINUE' where symbol=vsymbol and INS_DATE<>(select max(tradedate) from nasdaq_avg) ;
  commit;
  select count(*) into v_cnt from nasdaq.down_150_log where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg) and CATEGORY='R';
  if (v_cnt=1) then
  select FUNDA_POINTS,PRICE_POINTS,MN3,MN2,MN1,MX3,MX2,MX1 into v_FUNDA_POINTS,v_PRICE_POINTS,v_MN3,v_MN2,v_MN1,v_MX3,v_MX2,v_MX1 from nasdaq.down_150_log where symbol=vsymbol and tradedate=(select max(tradedate) from nasdaq_avg);
  update nasdaq.observer_log set CURR_FUNDA_POINTS=v_FUNDA_POINTS,CURR_PRICE_POINTS=v_PRICE_POINTS,MN3=v_MN3,MN2=v_MN2,MN1=v_MN1,MX3=v_MX3,MX2=v_MX2,MX1=v_MX1  where symbol=vsymbol;
  commit;
  else
  if (v_CATEGORY='R') then 
  v_funda_points:=Funda_point_fn(symbol=>vsymbol,ins_script=>'R150');
  v_price_points:=Price_point_fn(symbol=>vsymbol,ins_script=>'R150-Price');
  select x.c.val_1, x.c.val_2 into v_1min_price,v_1min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>1) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_2min_price,v_2min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>2) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_3min_price,v_3min_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MIN3',years=>3) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_1max_price,v_1max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>1) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_2max_price,v_2max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>2) c from dual ) x;
  select x.c.val_1, x.c.val_2 into v_3max_price,v_3max_date from (select Min_Max_Price_fn(in_symbol=>vsymbol,Minmax=>'MAX3',years=>3) c from dual ) x;
  DBMS_OUTPUT.put_line ('SYMBOL IS =' || vsymbol||' 1min price'||v_1min_price||'v_1max_price :'||v_1max_price);
  update nasdaq.observer_log set CURR_FUNDA_POINTS=v_funda_points,CURR_PRICE_POINTS=v_price_points,MN3=v_3min_price,MN2=v_2min_price,MN1=v_1min_price,MX3=v_3max_price,MX2=v_2max_price,MX1=v_1max_price  where symbol=vsymbol;
  commit;
  end if;
  end if;
  update nasdaq.observer_log set state='UPPERSEAL' where ((PROFIT_LOSS>30) or CURRENTPRICE>1.4*ONEFIFTYAVGPRI);
  insert into observer_log_log select * from observer_log where state='UPPERSEAL';
  delete from nasdaq.observer_log where  state='UPPERSEAL';
  COMMIT;
  else 
  update nasdaq.observer_log set state='ABSENT' where symbol=vsymbol;
  insert into observer_log_log select * from observer_log where symbol=vsymbol;
  delete from nasdaq.observer_log where symbol=vsymbol;
  commit;
  end if;
  
  
  END LOOP;
  close observe_cur;
END;