-- Note: Once Dataset is ready on Snowflake 
--1) Login to snowflake using my credentials
--2) use this script on snowflake worksheet.  



use database gold_sparknerwork;

--Q. How many total messages are being sent every day?
--A. List of messages-per-day is below

select distinct(cast("createdAt" as date )) as date, count("senderId") as Message_count
from messages
group by 1;


--Q. Are there any users that did not receive any message?
--A. Yes, there is one user who did not receive any meaasge.

select count(USER_ID) "Number of user did not receive message"
from user
where user_id not in (
    select distinct("receiverId")
    from messages
);


--Q. How many active subscriptions do we have today?
--A. There is no active subscription today

select *
from subscription
where date("startDate") = current_date() and "status" = 'Active';

--Q. Are there users sending messages without an active subscription?
--A. There are two user sending message without active status. 

select  count(distinct("senderId"))
from messages
where "senderId" not in(
    select USER_ID
    from subscription
    where "status" = 'Active'
);


--Q. How much is the average subscription amount?
--A. average subscription amount breakdown by year/month is below

select case
            when month("endDate") <=9 then concat(year("endDate"),'-0', month("endDate"))
            when month("endDate") >9 then concat(year("endDate"),'-', month("endDate")) 
        end as "year/month", 
        sum("amount")/count(user_id) as "average_subscription_amount"
from subscription
group by 1;


