--------------------------------------------------------
--  DDL for Function TCH_VOL_CNT
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."TCH_VOL_CNT" (in_symbol in varchar2,in_curr_tab  in varchar2)
RETURN number
is
v_number number;
rw_cnt number;
CNT_STATEMENT varchar2(1000);
CNT_STATEMENT2 varchar2(1000);
v_PRCNT_CHANGE number;
v_SYMBOL varchar2(60);
v_TCH_CNT number;


begin
CNT_STATEMENT:='select count(*) from '||in_curr_tab||' where symbol='''||in_symbol||''' and INSERT_DATE>SYSDATE-30';
EXECUTE IMMEDIATE CNT_STATEMENT into rw_cnt ;
DBMS_OUTPUT.PUT_LINE('row cnt  is : '||rw_cnt);
if (rw_cnt=0) then
v_TCH_CNT:=1;
else
CNT_STATEMENT2:='select MAX(TCH_CNT)+1 from '||in_curr_tab||' where symbol='''||in_symbol||''' and INSERT_DATE>SYSDATE-30';
EXECUTE IMMEDIATE CNT_STATEMENT2 into v_TCH_CNT ;
end if;
return v_TCH_CNT;
end;

/
