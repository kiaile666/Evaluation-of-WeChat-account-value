-- 微信好友归属的店长信息
WITH wechatfriend AS 
(SELECT 
    a.g_name,a.g_leader,b.user_group_id,
    b.name,c.user_id,c.name as dz_name,d.user_account_id,
    d.weixin_id,d.nickname,e.last_msg_time,e.follow_time
    FROM dim_dzs_dz_group_1d a
    INNER JOIN dim_dzs_dz_1d b ON a.id=b.user_group_id
    INNER JOIN dim_dzs_dz_account_1d c ON b.id=c.user_id
    INNER JOIN dim_cus_wechatfriend_1d d ON c.id=d.user_account_id 
    INNER JOIN ( SELECT * FROM dwd_dzs_userwechatfriend_1d 
                -- WHERE relieve_time IS NULL
                ) e ON d.weixin_id=e.weixin_id AND d.user_account_id=e.user_account_id),

-- 点击店长带参链接
visit_link_num AS 
(SELECT a.user_id,visit_link_num_90,visit_link_num_45,
    CASE WHEN visit_link_num_45 IS NOT NULL THEN visit_link_num_90-visit_link_num_45 ELSE visit_link_num_90 END AS visit_link_num_45_90
    FROM (SELECT user_id,COUNT(DISTINCT id_2) AS visit_link_num_90 FROM ads_bi_channel_analysis_detail_1d WHERE scene_name='店长朋友圈'AND DATE(ds)>=CURRENT_DATE-91 GROUP BY user_id) a
    LEFT JOIN (SELECT user_id,COUNT(DISTINCT id_2) AS visit_link_num_45 FROM ads_bi_channel_analysis_detail_1d WHERE scene_name='店长朋友圈'AND DATE(ds)> CURRENT_DATE -46 GROUP BY user_id) b ON a.user_id=b.user_id),

-- 与店长会话记录
wexinfriend_chat AS 
(SELECT talker,is_send,
    DATE(create_time) AS chat_date,
    ROW_NUMBER() OVER( PARTITION BY talker,DATE(create_time) ORDER BY create_time,id ) AS rank
    FROM dwd_dzzs_wechatmessage_1d_v 
    WHERE DATE(ds)= CURRENT_DATE -1 AND is_send IN (0,1)),

-- 与店长会话次数_主动咨询
weixin_initiative_chat AS 
(SELECT a.talker,initiative_chat_90,initiative_chat_45,
    CASE WHEN initiative_chat_45 IS NOT NULL THEN initiative_chat_90-initiative_chat_45 ELSE initiative_chat_90 END AS initiative_chat_45_90
    FROM (SELECT talker,COUNT(chat_date) AS initiative_chat_90 FROM wexinfriend_chat WHERE rank=1 AND is_send=0 AND chat_date>=CURRENT_DATE-91 GROUP BY talker) a
    LEFT JOIN (SELECT talker,COUNT(chat_date) AS initiative_chat_45 FROM wexinfriend_chat WHERE rank=1 AND is_send=0 AND chat_date>=CURRENT_DATE-46 GROUP BY talker) b ON a.talker=b.talker),

-- 与店长会话次数_被动回复
weixin_passive_chat AS 
(SELECT a.talker,passive_chat_90,passive_chat_45,
    CASE WHEN passive_chat_45 IS NOT NULL THEN passive_chat_90-passive_chat_45 ELSE passive_chat_90 END AS passive_chat_45_90
    FROM (SELECT talker,COUNT(chat_date) AS passive_chat_90 FROM wexinfriend_chat WHERE rank=1 AND is_send=1 AND chat_date>=CURRENT_DATE-91 GROUP BY talker) a
    LEFT JOIN (SELECT talker,COUNT(chat_date) AS passive_chat_45 FROM wexinfriend_chat WHERE rank=1 AND is_send=1 AND chat_date>=CURRENT_DATE-46 GROUP BY talker) b ON a.talker=b.talker),

-- 启动小程序次数
pri_visit_date AS 
(SELECT a.open_id,visit_date_90,visit_date_45,
    CASE WHEN visit_date_45 IS NOT NULL THEN visit_date_90-visit_date_45 ELSE visit_date_90 END AS visit_date_45_90
    FROM (SELECT open_id,COUNT( DISTINCT ds) AS visit_date_90 FROM ads_bi_private_visit_1d WHERE DATE(ds)>=CURRENT_DATE-91 GROUP BY open_id) a 
    LEFT JOIN (SELECT open_id,COUNT( DISTINCT ds) AS visit_date_45 FROM ads_bi_private_visit_1d WHERE DATE(ds)>=CURRENT_DATE-46 GROUP BY open_id) b ON a.open_id=b.open_id),

-- 私域近XX天支付
pri_total_pay AS 
(SELECT 
    a.cust_wx_id_fk,
    CASE WHEN pri_order_num_315 IS NOT NULL THEN pri_order_num_360-pri_order_num_315 ELSE pri_order_num_360 END  AS pri_order_num_315_360,
	CASE WHEN pri_pay_amount_315 IS NOT NULL THEN pri_pay_amount_360-pri_pay_amount_315 ELSE pri_pay_amount_360 END  AS pri_pay_amount_315_360,
    CASE WHEN pri_order_num_270 IS NOT NULL THEN pri_order_num_315-pri_order_num_270 ELSE pri_order_num_315 END  AS pri_order_num_270_315,
	CASE WHEN pri_pay_amount_270 IS NOT NULL THEN pri_pay_amount_315-pri_pay_amount_270 ELSE pri_pay_amount_315 END  AS pri_pay_amount_270_315,
    CASE WHEN pri_order_num_225 IS NOT NULL THEN pri_order_num_270-pri_order_num_225 ELSE pri_order_num_270 END  AS pri_order_num_225_270,
	CASE WHEN pri_pay_amount_225 IS NOT NULL THEN pri_pay_amount_270-pri_pay_amount_225 ELSE pri_pay_amount_270 END  AS pri_pay_amount_225_270,
    CASE WHEN pri_order_num_180 IS NOT NULL THEN pri_order_num_225-pri_order_num_180 ELSE pri_order_num_225 END  AS pri_order_num_180_225,
	CASE WHEN pri_pay_amount_180 IS NOT NULL THEN pri_pay_amount_225-pri_pay_amount_180 ELSE pri_pay_amount_225 END  AS pri_pay_amount_180_225,
    CASE WHEN pri_order_num_135 IS NOT NULL THEN pri_order_num_180-pri_order_num_135 ELSE pri_order_num_180 END  AS pri_order_num_135_180,
	CASE WHEN pri_pay_amount_135 IS NOT NULL THEN pri_pay_amount_180-pri_pay_amount_135 ELSE pri_pay_amount_180 END  AS pri_pay_amount_135_180,
    CASE WHEN pri_order_num_90 IS NOT NULL THEN pri_order_num_135-pri_order_num_90 ELSE pri_order_num_135 END  AS pri_order_num_90_135,
	CASE WHEN pri_pay_amount_90 IS NOT NULL THEN pri_pay_amount_135-pri_pay_amount_90 ELSE pri_pay_amount_135 END  AS pri_pay_amount_90_135,
    CASE WHEN pri_order_num_45 IS NOT NULL THEN pri_order_num_90-pri_order_num_45 ELSE pri_order_num_90 END  AS pri_order_num_45_90,
	CASE WHEN pri_pay_amount_45 IS NOT NULL THEN pri_pay_amount_90-pri_pay_amount_45 ELSE pri_pay_amount_90 END  AS pri_pay_amount_45_90,
    pri_order_num_45,pri_pay_amount_45
 	FROM (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_360, SUM(pay_amt) AS pri_pay_amount_360 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-361 GROUP BY cust_wx_id_fk) a
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_315, SUM(pay_amt) AS pri_pay_amount_315 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-316 GROUP BY cust_wx_id_fk) b ON a.cust_wx_id_fk=b.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_270, SUM(pay_amt) AS pri_pay_amount_270 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-271 GROUP BY cust_wx_id_fk) c ON a.cust_wx_id_fk=c.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_225, SUM(pay_amt) AS pri_pay_amount_225 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-226 GROUP BY cust_wx_id_fk) d ON a.cust_wx_id_fk=d.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_180, SUM(pay_amt) AS pri_pay_amount_180 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-181 GROUP BY cust_wx_id_fk) e ON a.cust_wx_id_fk=e.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_135, SUM(pay_amt) AS pri_pay_amount_135 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-136 GROUP BY cust_wx_id_fk) f ON a.cust_wx_id_fk=f.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_90, SUM(pay_amt) AS pri_pay_amount_90 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-91 GROUP BY cust_wx_id_fk) g ON a.cust_wx_id_fk=g.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pri_order_num_45, SUM(pay_amt) AS pri_pay_amount_45 FROM dwd_trd_order_all_1d WHERE order_domain='私域' AND DATE(pay_time)>=CURRENT_DATE-46 GROUP BY cust_wx_id_fk) h ON a.cust_wx_id_fk=h.cust_wx_id_fk),

-- 公域近XX天支付
pub_total_pay AS 
(SELECT 
    a.cust_wx_id_fk,
    CASE WHEN pub_order_num_315 IS NOT NULL THEN pub_order_num_360-pub_order_num_315 ELSE pub_order_num_360 END  AS pub_order_num_315_360,
	CASE WHEN pub_pay_amount_315 IS NOT NULL THEN pub_pay_amount_360-pub_pay_amount_315 ELSE pub_pay_amount_360 END  AS pub_pay_amount_315_360,
    CASE WHEN pub_order_num_270 IS NOT NULL THEN pub_order_num_315-pub_order_num_270 ELSE pub_order_num_315 END  AS pub_order_num_270_315,
	CASE WHEN pub_pay_amount_270 IS NOT NULL THEN pub_pay_amount_315-pub_pay_amount_270 ELSE pub_pay_amount_315 END  AS pub_pay_amount_270_315,
    CASE WHEN pub_order_num_225 IS NOT NULL THEN pub_order_num_270-pub_order_num_225 ELSE pub_order_num_270 END  AS pub_order_num_225_270,
	CASE WHEN pub_pay_amount_225 IS NOT NULL THEN pub_pay_amount_270-pub_pay_amount_225 ELSE pub_pay_amount_270 END  AS pub_pay_amount_225_270,
    CASE WHEN pub_order_num_180 IS NOT NULL THEN pub_order_num_225-pub_order_num_180 ELSE pub_order_num_225 END  AS pub_order_num_180_225,
	CASE WHEN pub_pay_amount_180 IS NOT NULL THEN pub_pay_amount_225-pub_pay_amount_180 ELSE pub_pay_amount_225 END  AS pub_pay_amount_180_225,
    CASE WHEN pub_order_num_135 IS NOT NULL THEN pub_order_num_180-pub_order_num_135 ELSE pub_order_num_180 END  AS pub_order_num_135_180,
	CASE WHEN pub_pay_amount_135 IS NOT NULL THEN pub_pay_amount_180-pub_pay_amount_135 ELSE pub_pay_amount_180 END  AS pub_pay_amount_135_180,
    CASE WHEN pub_order_num_90 IS NOT NULL THEN pub_order_num_135-pub_order_num_90 ELSE pub_order_num_135 END  AS pub_order_num_90_135,
	CASE WHEN pub_pay_amount_90 IS NOT NULL THEN pub_pay_amount_135-pub_pay_amount_90 ELSE pub_pay_amount_135 END  AS pub_pay_amount_90_135,
    CASE WHEN pub_order_num_45 IS NOT NULL THEN pub_order_num_90-pub_order_num_45 ELSE pub_order_num_90 END  AS pub_order_num_45_90,
	CASE WHEN pub_pay_amount_45 IS NOT NULL THEN pub_pay_amount_90-pub_pay_amount_45 ELSE pub_pay_amount_90 END  AS pub_pay_amount_45_90,
    pub_order_num_45,pub_pay_amount_45
 	FROM (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_360, SUM(pay_amt) AS pub_pay_amount_360 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-361 GROUP BY cust_wx_id_fk) a
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_315, SUM(pay_amt) AS pub_pay_amount_315 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-316 GROUP BY cust_wx_id_fk) b ON a.cust_wx_id_fk=b.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_270, SUM(pay_amt) AS pub_pay_amount_270 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-271 GROUP BY cust_wx_id_fk) c ON a.cust_wx_id_fk=c.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_225, SUM(pay_amt) AS pub_pay_amount_225 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-226 GROUP BY cust_wx_id_fk) d ON a.cust_wx_id_fk=d.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_180, SUM(pay_amt) AS pub_pay_amount_180 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-181 GROUP BY cust_wx_id_fk) e ON a.cust_wx_id_fk=e.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_135, SUM(pay_amt) AS pub_pay_amount_135 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-136 GROUP BY cust_wx_id_fk) f ON a.cust_wx_id_fk=f.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_90, SUM(pay_amt) AS pub_pay_amount_90 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-91 GROUP BY cust_wx_id_fk) g ON a.cust_wx_id_fk=g.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS pub_order_num_45, SUM(pay_amt) AS pub_pay_amount_45 FROM dwd_trd_order_all_1d WHERE order_domain='公域' AND DATE(pay_time)>=CURRENT_DATE-46 GROUP BY cust_wx_id_fk) h ON a.cust_wx_id_fk=h.cust_wx_id_fk),

--全域近XX天支付
all_total_pay AS 
( SELECT 
    a.cust_wx_id_fk,
    CASE WHEN all_order_num_315 IS NOT NULL THEN all_order_num_360-all_order_num_315 ELSE all_order_num_360 END  AS all_order_num_315_360,
	CASE WHEN all_pay_amount_315 IS NOT NULL THEN all_pay_amount_360-all_pay_amount_315 ELSE all_pay_amount_360 END  AS all_pay_amount_315_360,
    CASE WHEN all_order_num_270 IS NOT NULL THEN all_order_num_315-all_order_num_270 ELSE all_order_num_315 END  AS all_order_num_270_315,
	CASE WHEN all_pay_amount_270 IS NOT NULL THEN all_pay_amount_315-all_pay_amount_270 ELSE all_pay_amount_315 END  AS all_pay_amount_270_315,
    CASE WHEN all_order_num_225 IS NOT NULL THEN all_order_num_270-all_order_num_225 ELSE all_order_num_270 END  AS all_order_num_225_270,
	CASE WHEN all_pay_amount_225 IS NOT NULL THEN all_pay_amount_270-all_pay_amount_225 ELSE all_pay_amount_270 END  AS all_pay_amount_225_270,
    CASE WHEN all_order_num_180 IS NOT NULL THEN all_order_num_225-all_order_num_180 ELSE all_order_num_225 END  AS all_order_num_180_225,
	CASE WHEN all_pay_amount_180 IS NOT NULL THEN all_pay_amount_225-all_pay_amount_180 ELSE all_pay_amount_225 END  AS all_pay_amount_180_225,
    CASE WHEN all_order_num_135 IS NOT NULL THEN all_order_num_180-all_order_num_135 ELSE all_order_num_180 END  AS all_order_num_135_180,
	CASE WHEN all_pay_amount_135 IS NOT NULL THEN all_pay_amount_180-all_pay_amount_135 ELSE all_pay_amount_180 END  AS all_pay_amount_135_180,
    CASE WHEN all_order_num_90 IS NOT NULL THEN all_order_num_135-all_order_num_90 ELSE all_order_num_135 END  AS all_order_num_90_135,
	CASE WHEN all_pay_amount_90 IS NOT NULL THEN all_pay_amount_135-all_pay_amount_90 ELSE all_pay_amount_135 END  AS all_pay_amount_90_135,
    CASE WHEN all_order_num_45 IS NOT NULL THEN all_order_num_90-all_order_num_45 ELSE all_order_num_90 END  AS all_order_num_45_90,
	CASE WHEN all_pay_amount_45 IS NOT NULL THEN all_pay_amount_90-all_pay_amount_45 ELSE all_pay_amount_90 END  AS all_pay_amount_45_90,
    all_order_num_45,all_pay_amount_45
 	FROM (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_360, SUM(pay_amt) AS all_pay_amount_360 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-361 GROUP BY cust_wx_id_fk) a
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_315, SUM(pay_amt) AS all_pay_amount_315 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-316 GROUP BY cust_wx_id_fk) b ON a.cust_wx_id_fk=b.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_270, SUM(pay_amt) AS all_pay_amount_270 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-271 GROUP BY cust_wx_id_fk) c ON a.cust_wx_id_fk=c.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_225, SUM(pay_amt) AS all_pay_amount_225 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-226 GROUP BY cust_wx_id_fk) d ON a.cust_wx_id_fk=d.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_180, SUM(pay_amt) AS all_pay_amount_180 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-181 GROUP BY cust_wx_id_fk) e ON a.cust_wx_id_fk=e.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_135, SUM(pay_amt) AS all_pay_amount_135 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-136 GROUP BY cust_wx_id_fk) f ON a.cust_wx_id_fk=f.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_90, SUM(pay_amt) AS all_pay_amount_90 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-91 GROUP BY cust_wx_id_fk) g ON a.cust_wx_id_fk=g.cust_wx_id_fk
    LEFT JOIN (SELECT cust_wx_id_fk,COUNT(shop_tid) AS all_order_num_45, SUM(pay_amt) AS all_pay_amount_45 FROM dwd_trd_order_all_1d WHERE DATE(pay_time)>=CURRENT_DATE-46 GROUP BY cust_wx_id_fk) h ON a.cust_wx_id_fk=h.cust_wx_id_fk),

wechatfriend_detail AS 
(SELECT 
    a.g_name 
    ,a.name
    ,a.user_id 
    ,a.dz_name
    ,a.user_account_id 
    ,a.weixin_id 
    ,a.nickname 
    ,CASE WHEN i.first_member_time IS NULL THEN 0 ELSE 1 END  AS is_member 
    ,CASE WHEN i.first_attention_time IS NULL THEN 0 
    	   WHEN i.first_attention_time IS NOT NULL AND i.is_attention=0 THEN 1 
    	   WHEN i.first_attention_time IS NOT NULL AND i.is_attention=1 THEN 2 END AS is_attetion -- 是否关注公众号 未关注过:0 关注过但当前未关注:1 关注过且当前关注:2
    ,c.visit_link_num_45 
    ,c.visit_link_num_45_90 
    ,d.initiative_chat_45 
    ,e.passive_chat_45
    ,d.initiative_chat_45_90 
    ,e.passive_chat_45_90 
    ,f.visit_date_45
    ,f.visit_date_45_90 
    ,g.pri_order_num_45 
	,g.pri_pay_amount_45
    ,g.pri_order_num_45_90 
    ,g.pri_pay_amount_45_90
    ,g.pri_order_num_90_135 
    ,g.pri_pay_amount_90_135
    ,g.pri_order_num_135_180 
    ,g.pri_pay_amount_135_180
    ,g.pri_order_num_180_225 
    ,g.pri_pay_amount_180_225
    ,g.pri_order_num_225_270 
    ,g.pri_pay_amount_225_270
    ,g.pri_order_num_270_315 
    ,g.pri_pay_amount_270_315
   	,g.pri_order_num_315_360 
    ,g.pri_pay_amount_315_360 
    ,h.pub_order_num_45 
	,h.pub_pay_amount_45
    ,h.pub_order_num_45_90 
    ,h.pub_pay_amount_45_90
    ,h.pub_order_num_90_135 
    ,h.pub_pay_amount_90_135
    ,h.pub_order_num_135_180 
    ,h.pub_pay_amount_135_180
    ,h.pub_order_num_180_225 
    ,h.pub_pay_amount_180_225
    ,h.pub_order_num_225_270 
    ,h.pub_pay_amount_225_270
    ,h.pub_order_num_270_315 
    ,h.pub_pay_amount_270_315
   	,h.pub_order_num_315_360 
    ,h.pub_pay_amount_315_360 
    ,j.all_order_num_45 
	,j.all_pay_amount_45
    ,j.all_order_num_45_90 
    ,j.all_pay_amount_45_90
    ,j.all_order_num_90_135 
    ,j.all_pay_amount_90_135
    ,j.all_order_num_135_180 
    ,j.all_pay_amount_135_180
    ,j.all_order_num_180_225 
    ,j.all_pay_amount_180_225
    ,j.all_order_num_225_270 
    ,j.all_pay_amount_225_270
    ,j.all_order_num_270_315 
    ,j.all_pay_amount_270_315
   	,j.all_order_num_315_360 
    ,j.all_pay_amount_315_360 
    FROM wechatfriend a
    LEFT JOIN (SELECT union_id_fk,wx_id_fk,ma_open_id_fk FROM dim_pub_dw_user_id_1d WHERE DATE(ds)= CURRENT_DATE -1) b ON a.weixin_id=b.wx_id_fk    
    LEFT JOIN visit_link_num c ON b.union_id_fk=c.user_id    
    LEFT JOIN weixin_initiative_chat d ON a.weixin_id=d.talker
    LEFT JOIN weixin_passive_chat e ON a.weixin_id=e.talker 
    LEFT JOIN pri_visit_date f ON b.ma_open_id_fk=f.open_id   
    LEFT JOIN pri_total_pay g ON a.weixin_id=g.cust_wx_id_fk
    LEFT JOIN pub_total_pay h ON a.weixin_id=h.cust_wx_id_fk
    LEFT JOIN dwt_cus_dmp_user_tags_1d_nd i ON a.weixin_id=i.wx_id_fk
    LEFT JOIN all_total_pay j ON a.weixin_id=j.cust_wx_id_fk)

SELECT * FROM wechatfriend_detail ORDER BY weixin_id 

