--------------------------------------------------------
--  DDL for Procedure STOCK_SPLIT_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "NASDAQ"."STOCK_SPLIT_STATUS" 
is
v_TRADEDATE date;
v_curr_mon varchar2(10);
v_max_mon varchar2(10);
v_curr_year varchar2(10);
v_curr_date date;
v_down_mon varchar2(10);
v_down_pre_mon varchar2(10);
v_curr_date_flag number;
begin
select trunc(sysdate) into v_curr_date from dual;
v_curr_mon:=to_char(v_curr_date,'MM');
v_max_mon:=to_char(v_curr_date,'MM');
v_curr_year:=to_char(v_curr_date,'YYYY');
select max(tradedate) into v_TRADEDATE from nasdaq_hist;
DBMS_OUTPUT.PUT_LINE('current sysdate   is   '||v_curr_date);
DBMS_OUTPUT.PUT_LINE('*** current mon  is  **  '||v_curr_mon);
DBMS_OUTPUT.PUT_LINE('*** max mon  is  **  '||v_max_mon );
DBMS_OUTPUT.PUT_LINE('*** current year  is  **  '||v_curr_year);
DBMS_OUTPUT.PUT_LINE('max date from nasdaq_hist   is   '||v_TRADEDATE);
select count(*) into v_curr_date_flag from SPLIT_STOCK_DOWNLOAD_STATUS where INS_DATE=v_curr_date;
DBMS_OUTPUT.PUT_LINE('status flag   is   '||v_curr_date_flag);
if (v_curr_date_flag=0) then
v_down_mon:=v_curr_mon||'/01/'||v_curr_year;
v_down_pre_mon:=v_max_mon||'/01/'||v_curr_year;
DBMS_OUTPUT.PUT_LINE('final download mon   is   '||v_down_mon);
DBMS_OUTPUT.PUT_LINE('final download pre mon   is   '||v_down_pre_mon);
if (v_max_mon=v_curr_mon) then
insert into SPLIT_STOCK_DOWNLOAD_STATUS (DOWNLOAD_MON,INS_DATE) values (v_down_mon,v_curr_date);
commit;
else
insert into SPLIT_STOCK_DOWNLOAD_STATUS (DOWNLOAD_MON,INS_DATE) values (v_down_mon,v_curr_date);
insert into SPLIT_STOCK_DOWNLOAD_STATUS (DOWNLOAD_MON,INS_DATE) values (v_down_pre_mon,v_curr_date);
commit;
end if;
else
null;
end if;
end;

/
