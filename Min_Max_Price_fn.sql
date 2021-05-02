--------------------------------------------------------
--  DDL for Function MIN_MAX_PRICE_FN
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "NASDAQ"."MIN_MAX_PRICE_FN" (in_symbol in varchar2,Minmax in varchar2,years  IN number)
RETURN maxmin_fn_type
is
v_number number;
v_currentprice number;
v_date date;
rw_cnt number;

begin
if (Minmax='MIN') then
if (years=1) then
select currentprice,min(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,min(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-365  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
elsif (years=2) then
select currentprice,min(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,min(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-730  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
elsif (years=3) then
select currentprice,min(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,min(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-1095  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
else
null;
end if;
elsif (Minmax='MAX') then
if (years=1) then
select currentprice,max(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,max(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-365  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
elsif (years=2) then
select currentprice,max(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,max(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-730  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
elsif (years=3) then
select currentprice,max(tradedate) into v_currentprice,v_date from nasdaq_avg where (symbol,currentprice) in (select symbol,max(currentprice) from nasdaq_avg where symbol=in_symbol AND TRADEDATE>(SELECT MAX(TRADEDATE)-1095  FROM NASDAQ_AVG )  GROUP BY SYMBOL) group by currentprice;
else
null;
end if;
else
null;
end if;
return maxmin_fn_type(v_currentprice,v_date);
end;

/
