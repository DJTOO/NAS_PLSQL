--------------------------------------------------------
--  DDL for Procedure FUNDA_POINTS_PROC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "NASDAQ"."FUNDA_POINTS_PROC" 
is
in_symbol  varchar2(20);
v_symbol varchar2(60);
v_YEAR_REV2 number;
v_YEAR_REV1 number;
v_QUAT_REV3 number;
v_QUAT_REV2 NUMBER;
v_QUAT_REV1 number;
v_YEAR_EPS2 number;
v_YEAR_EPS1  number;
v_QUAT_EPS3 number;
v_QUAT_EPS2 number;
v_QUAT_EPS1 number;
v_YEAREPS_NEG_CNT number;
v_QUATEPS_NEG_CNT number;
v_YEAR_EPS_PROF  number;
v_QUAT_EPS_PROF number;
v_YEAR_REV_GRWRATE number;
v_YEAR_EPS_GRWRATE number;
v_QOQ_REV1 number;
v_QOQ_REV2 number;
v_QOQ_REV3 number;
v_QOQ_REV4 number;
v_QOQ_EPS1 number;
v_QOQ_EPS2 number;
v_QOQ_EPS3 number;
v_QOQ_EPS4 number;
v_LABEL varchar2(15);
v_TOTAL number;
v_TWOYEAR_PE number;
v_SECTOR varchar2(100);
v_INS_DATE  date;
v_level number;
v_count number;
v_rnk   number;

cursor C0  is select symbol from financials intersect select symbol from quickfs_quaterly;

begin
select max(tradedate) into v_INS_DATE from nasdaq_avg;
insert into funda_points_log select * from funda_points;
EXECUTE IMMEDIATE 'TRUNCATE TABLE funda_points ';
commit;
open C0;
LOOP
FETCH C0 into in_symbol;
EXIT WHEN C0%NOTFOUND;
DBMS_OUTPUT.PUT_LINE('symbol is   '||in_symbol);
        v_YEAR_REV2:=YEAR_REVENUE(in_symbol).VAL2;
v_YEAR_REV1:=YEAR_REVENUE(in_symbol).VAL1;
        v_QUAT_REV3:=QUAT_REVENUE(in_symbol).VAL3;
        v_QUAT_REV2:=QUAT_REVENUE(in_symbol).VAL2;
        v_QUAT_REV1:=QUAT_REVENUE(in_symbol).VAL1;
        v_YEAR_EPS2:=YEAR_EPS(in_symbol).VAL3;
        v_YEAR_EPS1:=YEAR_EPS(in_symbol).VAL2;
v_YEAREPS_NEG_CNT:=YEAR_EPS(in_symbol).VAL1;
        v_QUAT_EPS3:=QUAT_EPS(in_symbol).VAL4;
        v_QUAT_EPS2:=QUAT_EPS(in_symbol).VAL3;
v_QUAT_EPS1:=QUAT_EPS(in_symbol).VAL2;
v_QUATEPS_NEG_CNT:=QUAT_EPS(in_symbol).VAL1;
v_QOQ_REV1:=QOQ_REV(in_symbol).VAL1;
v_QOQ_REV2:=QOQ_REV(in_symbol).VAL2;
v_QOQ_REV3:=QOQ_REV(in_symbol).VAL3;
v_QOQ_REV4:=QOQ_REV(in_symbol).VAL4;
v_QOQ_EPS1:=QOQ_EPS(in_symbol).VAL1;
v_QOQ_EPS2:=QOQ_EPS(in_symbol).VAL2;
v_QOQ_EPS3:=QOQ_EPS(in_symbol).VAL3;
v_QOQ_EPS4:=QOQ_EPS(in_symbol).VAL4;
begin
        INSERT INTO funda_points(SYMBOL,INS_DATE,YEAR_REV2,YEAR_REV1,QUAT_REV3,QUAT_REV2,QUAT_REV1,YEAR_EPS2,YEAR_EPS1,QUAT_EPS3,QUAT_EPS2,QUAT_EPS1,YEAREPS_NEG_CNT,QUATEPS_NEG_CNT,QOQ_REV1,QOQ_REV2,QOQ_REV3,QOQ_REV4,QOQ_EPS1,QOQ_EPS2,QOQ_EPS3,QOQ_EPS4) VALUES (in_symbol,v_INS_DATE,v_YEAR_REV2,v_YEAR_REV1,v_QUAT_REV3,v_QUAT_REV2,v_QUAT_REV1,v_YEAR_EPS2,v_YEAR_EPS1,v_QUAT_EPS3,v_QUAT_EPS2,v_QUAT_EPS1,v_YEAREPS_NEG_CNT,v_QUATEPS_NEG_CNT,v_QOQ_REV1,v_QOQ_REV2,v_QOQ_REV3,v_QOQ_REV4,v_QOQ_EPS1,v_QOQ_EPS2,v_QOQ_EPS3,v_QOQ_EPS4);
        commit;
EXCEPTION
WHEN DUP_VAL_ON_INDEX
THEN
DBMS_OUTPUT.PUT_LINE('EXCEPTION is   '||in_symbol);
end;
        END LOOP;
        close C0;

end;

/
