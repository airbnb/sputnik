select count(distinct(userId)) as user_count, 'desktop' as platform,  ds
 from new_users
 group by ds