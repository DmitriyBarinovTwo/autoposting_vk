
class sql_query_manager(object):

    def __init__(self, ConfigSystem):
        super().__init__(ConfigSystem)
        ConfigVKQuery = ConfigSystem['VkApiParam']
        self.IdGroupQuery = ConfigVKQuery.get('GROUP_ID')

    def StatusNewPost(self):
        TextQuery = f"""SELECT  
        CASE WHEN COUNT(*)=0 THEN FALSE ELSE TRUE END as status 
        FROM {self.SchemaDB}.out_of_line_post
        where status = false
        and id_group = {self.IdGroupQuery}"""
        return TextQuery

    def GetHistoryPost(self):
        TextQuery = f"""select id_push,
        id_albom,
        id_pic,
        comment,
        line,
        time_push AT TIME ZONE 'UTC-0' as time_push
        from {self.SchemaDB}.history_post
        where id_push = (select max(id_push) from {self.SchemaDB}.history_post)
        and id_group = {self.IdGroupQuery}"""
        return TextQuery
    
    # переработать запрос
    def GetNewLinePost(self):
        TextQuery = f"""SELECT id_albom, 
                id_photo,
                comment,
                union_photos,
                CAST(data AS DATE) as date

                FROM {self.SchemaDB}.out_of_line_post
                where status = false
                and id_group = {self.IdGroupQuery}
                LIMIT 1"""

        return TextQuery



    def ChahgeStatusNewPost(self):
        TextQuery = f"""
            UPDATE {self.SchemaDB}.out_of_line_post
            SET status = TRUE
            WHERE id_row = (select min(id_row) FROM {self.SchemaDB}.out_of_line_post
            where status = false and id_group = {self.IdGroupQuery})
            and id_group = {self.IdGroupQuery}
            """
        return TextQuery

    def CountPostDay(self):
        TextQuery = f"""
            SELECT count(*) 
            FROM {self.SchemaDB}.line_post
            where id_day = EXTRACT(ISODOW FROM NOW())
            and id_group = {self.IdGroupQuery}
            """
        return TextQuery

    def GetTimePostFromLine(self):
        TextQuery = f"""select max(time_push AT TIME ZONE 'UTC-3') as time_push
        from {self.SchemaDB}.history_post
        where id_group = {self.IdGroupQuery}
        limit 1"""
        return TextQuery

    def GetInfoPostSQL(self):
            TextQuery = f"""with    tb as
            (
            SELECT id_albom,
            EXTRACT(ISODOW FROM time_push) as day
            FROM {self.SchemaDB}.history_post
            WHERE CAST(time_push AS DATE) = CAST(NOW() AS DATE)
            and id_group = {self.IdGroupQuery}
            ),

            tb2 as
            (
            SELECT id_day, line_post.id_albom as id_am, union_photos as up, count_photo
            FROM {self.SchemaDB}.line_post
			left join tb ON tb.day = line_post.id_day and tb.id_albom = line_post.id_albom
            where finish_date is null and
			id_day = EXTRACT(ISODOW FROM NOW()) and
			CASE WHEN tb.id_albom = {self.SchemaDB}.line_post.id_albom THEN TRUE ELSE FALSE END = false
            and id_group = {self.IdGroupQuery}
            )

           
            select DISTINCT id_am as id_albom,
            id_day,
            total_photo,
            union_photos,
            count_photo
            from (SELECT *,
            ROW_NUMBER() OVER(PARTITION BY id_albom ORDER BY date DESC) as actual_row
            from {self.SchemaDB}.total_photos
			left join tb2 ON  {self.SchemaDB}.total_photos.id_albom = tb2.id_am
            WHERE id_group = {self.IdGroupQuery} and total_photos.union_photos = tb2.up) as t
         	Limit = 1 
            """
            return TextQuery
    
    def GetIdAlbomNew(self):
        TextQuery = f"""SELECT id_day, 
            id_albom, 
            union_photos,
            count_photo
            FROM {self.SchemaDB}.line_post
            where finish_date is null and
            id_day = EXTRACT(ISODOW FROM NOW())
            and id_group = {self.IdGroupQuery}
            LIMIT 1"""
        return TextQuery


    def GetIdPic(self,num):
            TextQuery = f"""with times as  (SELECT max(date) 
            FROM {self.SchemaDB}.total_photos
            where id_albom = {num}
            and id_group = {self.IdGroupQuery}),
            hp as (SELECT id_pic, time_push, id_albom
            FROM {self.SchemaDB}.history_post
            where id_albom = {num}
            and id_group = {self.IdGroupQuery}
            ),
            l as (SELECT id_albom, total_photo
                from {self.SchemaDB}.total_photos
                where date = (SELECT max(date) 
                    FROM {self.SchemaDB}.total_photos
                    where id_albom = {num}) and id_albom = {num}
                    and id_group = {self.IdGroupQuery}
                )
                
            select distinct id_pic, total_photo
            from hp
            LEFT JOIN l on l.id_albom = hp.id_albom
            where time_push > (select * from times)
            """
            return TextQuery

    def GetIdCom(self, InfoPost, photo, check: bool):
        """
        Получить историю постов
        """

        
        # получаем очередь комментариев
        if check == True:
            TextQuery = f"""SELECT comment
            from {self.SchemaDB}.comment_photo
            where id_albom = {InfoPost['id_albom'][0]} and
            id_photo = {photo.split('-')[0] if InfoPost['union_photos'][0] == True else photo}
            and id_group = {self.IdGroupQuery}
            and union_photos = {InfoPost['union_photos'][0]}
            """
         
            return TextQuery
        
        # получаем все опубликованные ранее
        else:
            TextQuery = f"""SELECT comment, 
	        count(comment) as count 
            FROM {self.SchemaDB}.history_post
            where id_albom = {InfoPost['id_albom'][0]} and id_pic = {photo.split('-')[0] if InfoPost['union_photos'][0] == True else photo}
            and id_group = {self.IdGroupQuery}
            and union_photos = {InfoPost['union_photos'][0]}
            GROUP BY comment"""

            return TextQuery