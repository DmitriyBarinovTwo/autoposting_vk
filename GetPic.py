from random import randint, choice
from pathlib import Path
import os
import yaml
from yaml.loader import SafeLoader

class GetPicture(object):

    def __init__(self, ConfigSystem):
        super().__init__(ConfigSystem)
        ConfigAlbom = ConfigSystem['LinkAlbom']
        self.WithoutUnion = ConfigAlbom.get('ALBOM_WITHOUT_UNION')
        self.WithUnion = ConfigAlbom.get('ALBOM_WITH_UNION')
   
    def GetPicUnionTrue(self,InfoPost, IdPhotoPush, NewPost=False):

        BoxNewPic = []
        DirAlbom = self.WithUnion + str('/') + str(InfoPost['id_albom'][0])
       
        if NewPost == True:
            NewIdPic = InfoPost['id_photo'][0]
        else:
            NewIdPic = choice([i for i in range(1,InfoPost['total_photo'][0]) if i not in IdPhotoPush])
        
        
        for filename in os.listdir(DirAlbom):
            if str(NewIdPic) == filename.split('-')[0]:
                BoxNewPic.append(filename.split('.')[0])

        return NewIdPic, BoxNewPic, DirAlbom
    

    def GetPicUnionFalse(self,InfoPost, IdPhotoPush, NewPost=False):

        BoxNewPic = []
        

        DirAlbom = self.WithoutUnion + str('/') + str(InfoPost['id_albom'][0])
        
        if NewPost == True:
            NewIdPic = InfoPost['id_photo'][0]
            for filename in os.listdir(DirAlbom):
                if str(NewIdPic) == filename.split('.')[0]:
                    BoxNewPic.append(filename.split('.')[0])
        else:
            for num in range(InfoPost['count_photo'][0]):
                NewIdPic = choice([i for i in range(1,InfoPost['total_photo'][0]) if i not in IdPhotoPush])
                BoxNewPic.append(NewIdPic)
                IdPhotoPush.append(NewIdPic)

        return BoxNewPic, DirAlbom