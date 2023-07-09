import requests


class vk_manager(object):

        def __init__(self, ConfigSystem: dict):
                ConfigVK = ConfigSystem['VkApiParam']
                self.ACCESS_TOKEN = ConfigVK.get('ACCESS_TOKEN')
                self.GROUP_ID = ConfigVK.get('GROUP_ID')
                self.VERSION = ConfigVK.get('VERSION')


        def LoadVkСontent(self, PhotoPath,Date, Comment):
                response = requests.get('https://api.vk.com/method/photos.getWallUploadServer',
                params={'access_token': self.ACCESS_TOKEN,
                        'group_id': self.GROUP_ID,
                        'v': self.VERSION})
                upload_url = response.json()['response']['upload_url']

                BoxPhotoLink = []
                
                for i in range(len(PhotoPath)):

                # Формируем данные параметров для сохранения картинки на сервере
                        request = requests.post(upload_url, files={'photo': open(PhotoPath[i], "rb")})

                        # Сохраняем картинку на сервере и получаем её идентификатор
                        photo_id = requests.get('https://api.vk.com/method/photos.saveWallPhoto',
                        params = {'access_token': self.ACCESS_TOKEN,
                                'group_id': self.GROUP_ID,
                                'photo': request.json()["photo"],
                                'server': request.json()['server'],
                                'hash': request.json()['hash'],
                                'v': self.VERSION}
                        )
                        a = 'photo' + str(photo_id.json()['response'][0]['owner_id']) + '_' + str(photo_id.json()['response'][0]['id'])
                        BoxPhotoLink.append(a)

                PhotoLink = ",".join(BoxPhotoLink)

                # # Формируем параметры для размещения картинки в группе и публикуем её
                if Comment != 'NULL':
                        params = {'access_token': self.ACCESS_TOKEN,
                        'owner_id': -self.GROUP_ID,
                        'from_group': 1,
                        'message': Comment,
                        'attachments': PhotoLink,
                        'publish_date' : Date,
                        'v': self.VERSION}
                else:
                        params = {'access_token': self.ACCESS_TOKEN,
                        'owner_id': -self.GROUP_ID,
                        'from_group': 1,
                        'attachments': PhotoLink,
                        'publish_date' : Date,
                        'v': self.VERSION}

                
                requests.get('https://api.vk.com/method/wall.post', params)
