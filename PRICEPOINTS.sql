--------------------------------------------------------
--  DDL for Function PRICEPOINTS
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."PRICEPOINTS" (in_view in number,in_symbol in varchar2,in_INSERTDATE IN DATE)
RETURN number
is
v_number number;
v_cnt number;
rw_cnt number;
query1 varchar2(1000);
query2 varchar2(1000);
begin
v_cnt:=0;
if (in_view=01)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_01','nasbull_pri2_01'));
elsif (in_view=15)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_15','nasbull_pri2_15'));
elsif (in_view=516)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_516','nasbull_pri2_516'));
elsif (in_view=1625)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_1625','nasbull_pri2_1625'));
elsif (in_view=2575)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_2575','nasbull_pri2_2575'));
elsif (in_view=75)
then
select count(*) into v_cnt from (select distinct inser_script from pri_log where symbol=in_symbol and insert_date=in_INSERTDATE and inser_script in ('nasbull_pri_75','nasbull_pri2_75'));
else
null;
end if;
v_number:=v_cnt*2;
return trunc(v_number,2);
end;

/
