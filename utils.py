import pandas as pd
from mananger_DB import manager_DB
from sql_query import sql_query_manager as SqlMan
from logi import LogManagerPost as Log
from loadVkConnent import vk_manager as VkMan
from GetPic import GetPicture as GP
from datetime import timedelta, datetime, timezone
from random import randint
import os
import sys
from pathlib import Path 
import time
import logging
import os.path

logger = logging.getLogger("LogManagerPost")
logger.info("from utils.py")

class ManagerPost(manager_DB,GP,SqlMan,VkMan,Log):
    "класс для управленеия постановкой постов"
    
    def CheckTime (self):
        """определяем сколько разница по времени должна быть
        исходя из количества планируемых постов
        возвращает True если необходимое время прошло"""
        
        CountPost = str(pd.read_sql(SqlMan.CountPostDay(self), self.conn)['count'][0])
        TimeIntervalDict = {'2': 13, 
                    '3': 5,
                    '4': 4,
                    }
        

        Today = datetime.now()
        TimePost = pd.read_sql(SqlMan.GetTimePostFromLine(self), self.conn)['time_push'][0]
        
        if (Today - TimePost) > timedelta(hours=TimeIntervalDict[CountPost]):
            Log.TimeCorrect(self)
            return 
        else: 
            Log.TimeLess(self,Today,TimePost,timedelta(hours=TimeIntervalDict[CountPost]))
            sys.exit()

    
  
    def GetInfoPost(self):
            """Проверка количество уже опубликованных фото.
            Если все фото почти опубликованы, обнуляем в БД запись с новым количеством фото"""

            InfoPost = pd.read_sql(SqlMan.GetInfoPostSQL(self), self.conn)
            if InfoPost.shape[0] == 0:
                Log.AllAlbomPush(self)
                sys.exit()
            IdAlbom = InfoPost['id_albom'][0]
            IdPhotoPush = pd.read_sql(SqlMan.GetIdPic(self,  IdAlbom), self.conn)
            ListPhotoPush = IdPhotoPush['id_pic'].to_list()
            
            BoolCheck = (len(IdPhotoPush) + InfoPost['count_photo'][0]) >= InfoPost['total_photo'][0]
            
            # переопределяем количество фото в таблице
            if  BoolCheck == True:
                Date = datetime.now(timezone.utc)
       
               
                if InfoPost['union_photos'][0] == False:
                    AlbomDir = Path(self.WithoutUnion, str(IdAlbom))
                    CountPhotoFromAlbom = len([name for name in os.listdir(AlbomDir) if os.path.isfile(os.path.join(AlbomDir, name))])
                else:
                    AlbomDir = Path(self.WithUnion, str(IdAlbom))
                    CountPhotoFromAlbom = len(set([filename.split('-')[0] for filename in os.listdir(AlbomDir)]))
    
                    
                LoadConnent = [[IdAlbom, CountPhotoFromAlbom, 'TIMESTAMP WITH TIME ZONE ', self.IdGroupQuery ,InfoPost['union_photos'][0]]]

                manager_DB.UploadRowDB(self,self.ConfigTable['total_photos'],LoadConnent, Date) 

                Log.NewCountPhoto(self,IdAlbom,CountPhotoFromAlbom)
                ListPhotoPush = []

            
            return InfoPost, ListPhotoPush



    def GetCommment(self, InfoPost, BoxNewPic):
        "определяем комменатрий для публикации"

        try:
            CommentDf = pd.DataFrame(\
                    pd.read_sql(SqlMan.GetIdCom(self,InfoPost, BoxNewPic, True), self.conn)['comment'][0].split(';'),\
                    columns=['comment'])
        except IndexError:
            comment = 'NULL'
            return  comment
        
        # вытаскиваем все комментарии
        CommentHistory = pd.read_sql(SqlMan.GetIdCom(self,InfoPost, BoxNewPic, False), self.conn)

        # считаем комментарии
        CommentBox = CommentDf.merge(CommentHistory, on='comment', how='left').fillna(0).sort_values(by='count').reset_index(drop=True)
        Log.CommentPhoto(self, CommentBox)

        return  CommentBox['comment'][0]

    def CommitPost(self):
        
        # проверяем новые посты и время
        ManagerPost.CheckTime(self)

        InfoPost = pd.read_sql(SqlMan.GetNewLinePost(self), self.conn)
        today = datetime.now()
        

        # сценарий если новая фото
        if InfoPost[InfoPost['date']==today.date()].shape[0] != 0:

            Line = False
            if InfoPost['comment'][0] != None:
                Comment = f"{InfoPost['comment'][0]}"
            else:
                Comment = 'NULL'
            
            
            if InfoPost['union_photos'][0] == True:
                NewIdPic, BoxNewPic, DirAlbom = GP.GetPicUnionTrue(self,InfoPost,[], True)
            else:
                BoxNewPic, DirAlbom = GP.GetPicUnionFalse(self,InfoPost,[],True)
            
            self.cursor.execute(SqlMan.ChahgeStatusNewPost(self))
            self.conn.commit()

        else:
            # сценарий если в рамках линии
            Log.NewPostNotExist(self)
            InfoPost, IdPhotoPush = ManagerPost.GetInfoPost(self)
            Line = True


            if InfoPost['union_photos'][0] == True:
                NewIdPic, BoxNewPic, DirAlbom = GP.GetPicUnionTrue(self,InfoPost,IdPhotoPush)
            else:
                BoxNewPic, DirAlbom = GP.GetPicUnionFalse(self,InfoPost,IdPhotoPush)

            
            # получаем номер фото и коммент
            Log.CommentPhoto(self, BoxNewPic)
            Comment = ManagerPost.GetCommment(self,InfoPost, BoxNewPic[0])

        # определяем время
        minute = randint(5, 59)
        Date = datetime(today.year, today.month, today.day, today.hour, minute)

        BoxWithPath = []

        # отображаем время публикации в БД (history_post)
        if InfoPost['union_photos'][0] == False:
            for i in range(len(BoxNewPic)):
                LoadConnent = [
                [InfoPost['id_albom'][0], 
                BoxNewPic[i],
                Comment.strip(),
                Line, 
                'TIMESTAMP WITH TIME ZONE ',
                self.GroupIdForDB,
                InfoPost['union_photos'][0]]
                ]
                manager_DB.UploadRowDB(self, self.ConfigTable['history_post'],LoadConnent, Date)
                Log.PostUploudDB(self)
                BoxWithPath.append(DirAlbom + str('/') + str(BoxNewPic[i]) + str('.jpg'))
        else:
                LoadConnent = [
                [InfoPost['id_albom'][0], 
                NewIdPic,
                Comment.strip(),
                Line, 
                'TIMESTAMP WITH TIME ZONE ',
                self.GroupIdForDB,
                InfoPost['union_photos'][0]]
                ]
            
                manager_DB.UploadRowDB(self, self.ConfigTable['history_post'],LoadConnent, Date)
                Log.PostUploudDB(self)

                for i in range(len(BoxNewPic)):
                    BoxWithPath.append(DirAlbom + str('/') + str(BoxNewPic[i]) + str('.jpg'))
        Log.InfoPostLog(self,InfoPost['id_albom'][0],BoxNewPic,Comment,Date)
        manager_DB.CoonClose(self)
        unixtime = time.mktime(Date.timetuple())
        VkMan.LoadVkСontent(self,BoxWithPath,unixtime,Comment)
        Log.PostUploudVK(self)

        return 
