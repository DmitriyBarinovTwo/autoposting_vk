import logging
import logging_loki


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("LogManagerPost")
logger.info("from logi.py")


class LogManagerPost(object):


    def __init__(self, ConfigSystem):
        super().__init__(ConfigSystem)

    def TimeCorrect(self):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : "Прошло достаточно времени после последний публикации"})

    def NewPostNotExist(self):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : "Отсутствуют посты вне очереди"})

    def TimeLess(self, Time,TimePost,delta):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : "Прошло недостаточно времени после последний публикации"})
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : f"Время сейчас: {Time} ; Время поста: {TimePost} ; Время ожидания {delta}"})

    def PostUploudDB(self):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : "Пост отображен в базе данных"})

    def PostUploudVK(self):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" :"Пост поставлен в очередь на публикацию в ВК"})

    def NewCountPhoto(self, NumAlbom, CountPhoto):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : f"В альбоме {NumAlbom} определено новое количество фото {CountPhoto}"})
    
    def InfoPostLog(self, NumAlbom, CountPhoto, com, time):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : f"Опубликован альбом {NumAlbom}, фото {CountPhoto}, комментарий {com}, время {time}"})

    def CommentPhoto(self,df):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : df})

    def AllAlbomPush(self):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : "Все альбомы опубликованы"})
    
    def MessageErrorShare(self,ex):
        logger.info({"ID_GROUP" : self.IdGroupQuery, "message" : f"Ошибка. {ex}"}, exc_info=True)