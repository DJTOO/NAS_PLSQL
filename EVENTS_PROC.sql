--------------------------------------------------------
--  DDL for Procedure EVENTS_PROC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "NASDAQ"."EVENTS_PROC" (TRADEDATE  IN date default null)
is
in_symbol  varchar2(20);
v_symbol varchar2(60);
v_INSERT_DATE date;
in_INSERT_DATE date;
v_V01      number;
v_V15      number;
v_V516      number;
v_V1625      number;
v_V2575      number;
v_V75      number;
v_P01      number;
v_P15      number;
v_P516      number;
v_P1625      number;
v_P2575      number;
v_P75      number;
v_VTOP     number;
v_TOTAL    number;
v_tradedate1 date;
v_tradedate  date;
v_level number;
v_count number;
v_rnk   number;


cursor C_symbol(v_TRADEDATE1 date) is select symbol,insert_date from events where insert_date=v_TRADEDATE1;

begin

if (TRADEDATE is null)
 then
  select max(tradedate) into v_tradedate1  from nasdaq.nasdaq_hist;
  --select current_load_date into v_tradedate1 from nse.load_date;
  else
   v_TRADEDATE1:=TRADEDATE;
end if;

select count(*) into v_count from nasdaq.EVENTS where INSERT_DATE=v_TRADEDATE1;
if (v_count=0) then
        insert into events (symbol,insert_date)  select distinct symbol,insert_date from vol_log where insert_date=v_TRADEDATE1 union select distinct symbol,insert_date from pri_log where insert_date=v_TRADEDATE1 union select distinct symbol,tradedate as v from vol_toppers_hist where tradedate=v_TRADEDATE1;
        commit;
        open C_symbol(v_TRADEDATE1);
        LOOP
        FETCH C_symbol into in_symbol,in_INSERT_DATE;
        EXIT WHEN C_symbol%NOTFOUND;
DBMS_OUTPUT.PUT_LINE('symbol is   '||in_symbol);
v_V01:=volpoints(01,in_symbol,in_INSERT_DATE);
        v_V15:=volpoints(15,in_symbol,in_INSERT_DATE);
        v_V516:=volpoints(516,in_symbol,in_INSERT_DATE);
        v_V1625:=volpoints(1625,in_symbol,in_INSERT_DATE);
        v_V2575:=volpoints(2575,in_symbol,in_INSERT_DATE);
        v_V75:=volpoints(75,in_symbol,in_INSERT_DATE);
        v_P01:=pricepoints(01,in_symbol,in_INSERT_DATE);
        v_P15:=pricepoints(15,in_symbol,in_INSERT_DATE);
        v_P516:=pricepoints(516,in_symbol,in_INSERT_DATE);
v_P1625:=pricepoints(1625,in_symbol,in_INSERT_DATE);
v_P2575:=pricepoints(2575,in_symbol,in_INSERT_DATE);
v_P75:=pricepoints(75,in_symbol,in_INSERT_DATE);
v_VTOP:=VTOP(in_symbol,in_INSERT_DATE);
v_TOTAL:=v_V01+v_V15+v_V516+v_V1625+v_V2575+v_V75+v_P01+v_P15+v_P516+v_P1625+v_P2575+v_P75+v_VTOP;
        update events set V01=v_V01,V15=v_V15,V516=v_V516,V1625=v_V1625,V2575=v_V2575,V75=v_V75,P01=v_P01,P15=v_P15,P516=v_P516,P1625=v_P1625,P2575=v_P2575,P75=v_P75,VTOP=v_VTOP,TOTAL=v_TOTAL where symbol=in_symbol and INSERT_DATE=in_INSERT_DATE;
commit;
        END LOOP;
        close C_symbol;

else
        DBMS_OUTPUT.PUT_LINE('DATE already in Events');
end if;
end;

/
