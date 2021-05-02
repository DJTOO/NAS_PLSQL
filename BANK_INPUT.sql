--------------------------------------------------------
--  DDL for Procedure BANK_INPUT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "NASDAQ"."BANK_INPUT" (amnt IN number )
is
v_close_amnt number;
begin
select close_amnt into v_close_amnt from cash where trans_id=(select max(trans_id) from cash);
insert into cash (trans_id,open_amnt,symbol,expirydate,tradedate,Net_loss_profit,Bank_amnt,close_amnt,comments,OVERALL_UNITS,OVERALL_STK_DEDUCT) values (cash_trans_seq.nextval,v_close_amnt,'BANK',to_date('01-01-3020','dd-mm-yyyy'),sysdate,0,amnt,v_close_amnt+amnt,'frm bank',0,0);
commit;
end;

/
