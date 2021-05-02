--------------------------------------------------------
--  DDL for Function TCH_CNT
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."TCH_CNT" (in_symbol in varchar2,in_curr_tab  in varchar2)
RETURN NAS_TCH_CNT_TYPE
is
v_number number;
rw_cnt number;
rw_cnt2 number;
v_last_price number;
v_curr_price number;
v_price number;
CNT_STATEMENT varchar2(1000);
CNT_STATEMENT2 varchar2(1000);
CNT_STATEMENT3 varchar2(1000);
SEQ_CNT_STATEMENT varchar2(1000);
SEQ_CNT_STATEMENT1 varchar2(1000);
v_PRCNT_CHANGE number;
v_SYMBOL varchar2(60);
v_TCH_CNT number;
v_SEQ_CNT number;
v_INSERT_DATE date;
seq_cnt number;


begin
SEQ_CNT_STATEMENT:='select count(*) from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
EXECUTE IMMEDIATE SEQ_CNT_STATEMENT into seq_cnt ;
DBMS_OUTPUT.PUT_LINE('seq row cnt  is : '||seq_cnt);


CNT_STATEMENT:='select count(*) from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-7';
EXECUTE IMMEDIATE CNT_STATEMENT into rw_cnt ;
DBMS_OUTPUT.PUT_LINE('row cnt  is : '||rw_cnt);
if (rw_cnt=0) then
if (seq_cnt=0) then
v_SEQ_CNT:=1;
v_TCH_CNT:=1;
else
v_TCH_CNT:=1;
SEQ_CNT_STATEMENT1:='select max(SEQUENCE)+1 from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
EXECUTE IMMEDIATE SEQ_CNT_STATEMENT1 into v_SEQ_CNT;
end if;
else
  select currentprice into v_curr_price from (select  TRADEDATE,CURRENTPRICE  from PRICE_INTERIM where symbol=in_symbol  order by TRADEDATE desc) where rownum<2;
  CNT_STATEMENT2:='select currentprice from (SELECT INS_DATE,TCH_CNT,currentprice  FROM '||in_curr_tab||' where symbol='''||in_symbol||''' ORDER BY INS_DATE desc) where rownum<2';
  EXECUTE IMMEDIATE CNT_STATEMENT2 into v_last_price;
  if (in_curr_tab<>'all_negitive') then
    if (v_curr_price>=v_last_price )then
      CNT_STATEMENT3:='select (TCH_CNT)+1 from (SELECT INS_DATE,TCH_CNT,currentprice  FROM '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-7  ORDER BY INS_DATE desc) where rownum<2';
      EXECUTE IMMEDIATE CNT_STATEMENT3 into v_TCH_CNT ;
  SEQ_CNT_STATEMENT1:='select max(SEQUENCE) from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
  EXECUTE IMMEDIATE SEQ_CNT_STATEMENT1 into v_SEQ_CNT;
  DBMS_OUTPUT.PUT_LINE('CURRENT PRICE MORE : '||v_SEQ_CNT);
    else
      v_TCH_CNT:=1;
  SEQ_CNT_STATEMENT1:='select max(SEQUENCE)+1 from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
  EXECUTE IMMEDIATE SEQ_CNT_STATEMENT1 into v_SEQ_CNT;
    end if;
  else
    if (v_curr_price<=v_last_price )then
      CNT_STATEMENT3:='select (TCH_CNT)+1 from (SELECT INS_DATE,TCH_CNT,currentprice  FROM '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-7  ORDER BY INS_DATE desc) where rownum<2';
      EXECUTE IMMEDIATE CNT_STATEMENT3 into v_TCH_CNT ;
  SEQ_CNT_STATEMENT1:='select max(SEQUENCE) from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
  EXECUTE IMMEDIATE SEQ_CNT_STATEMENT1 into v_SEQ_CNT;
    else
      v_TCH_CNT:=1;
  SEQ_CNT_STATEMENT1:='select max(SEQUENCE)+1 from '||in_curr_tab||' where symbol='''||in_symbol||''' and INS_DATE>SYSDATE-60';
  EXECUTE IMMEDIATE SEQ_CNT_STATEMENT1 into v_SEQ_CNT;
    end if;
  end if;
end if;
return NAS_TCH_CNT_TYPE (v_TCH_CNT,v_SEQ_CNT);

end;

/
