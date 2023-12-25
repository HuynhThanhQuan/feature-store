-- Số lượng KH theo từng trạng thái (HĐ, ngủ đông, đóng băng);
select cust_status, count(distinct customer_cde) as slkh
from DW_ANALYTICS.DATA_RPT_SLKH_CUST_PRODUCT_ALL
group by cust_status;

-- Số lượng KH sử dụng cả CASA và Thẻ;
select count(distinct customer_cde) as slkh
from DW_ANALYTICS.DATA_RPT_SLKH_CUST_PRODUCT_ALL
where tktt = 1 and credit = 1 ;

-- Số lượng KH CASA không sử dụng Thẻ;
select count(distinct customer_cde) as slkh
from DW_ANALYTICS.DATA_RPT_SLKH_CUST_PRODUCT_ALL
where tktt = 1 and credit = 0 ;

-- Số lượng KH inactive của CASA: Khách hàng không có bất kỳ giao dịch trong 12 tháng gần nhất, ít nhất sử dụng 1 tài khoản CASA, ít nhất 1 giao dịch trong vòng 36 tháng
select count(distinct customer_cde) as slkh
from DW_ANALYTICS.DATA_RPT_SLKH_CUST_PRODUCT_ALL
where tktt=1
and cust_status in ('NGU DONG', 'DONG BANG');


-- Số lượng KH inactive của Thẻ tín dụng: Khách hàng sở hữu ít nhất 1 thẻ tín dụng và không có bất kỳ giao dịch Thẻ tín dụng nào trong 06 tháng gần nhất;
select count(distinct customer_cde) as slkh
from DW_ANALYTICS.DATA_RPT_SLKH_CUST_PRODUCT_ALL
where credit=1
and cust_status in ('NGU DONG', 'DONG BANG');