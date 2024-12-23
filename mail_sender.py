import smtplib
from email.mime.text import MIMEText
from email.header import Header
import ssl

class mail_send():

        def __init__(self, ConfigSystem: dict):
                super().__init__(ConfigSystem)
                ConfigMAIL = ConfigSystem['MailSender']
                self.name_group = ConfigMAIL.get('NAME_GROUP')
                self.server = ConfigMAIL.get('SERVER')
                self.port = ConfigMAIL.get('PORT')
                self.username = ConfigMAIL.get('USERNAME')
                self.password = ConfigMAIL.get('PASSWORD')
                self.from_address = ConfigMAIL.get('FROM_ADDRESS')
                self.to_address = ConfigMAIL.get('TO_ADDRESS')

        def ErrorMessageMail(self, errorMessage):
                # Тема и тело письма
                subject = f"Ошибка от группы {self.name_group}"
                body = f"Группа {self.name_group}. Расшифровка: \n {errorMessage}"


                # Кодировка темы и тела сообщения
                msg = MIMEText(body.encode('utf-8'), _charset='utf-8')
                msg['Subject'] = Header(subject, 'utf-8')
                msg['From'] = self.from_address
                msg['To'] = self.to_address


                with smtplib.SMTP_SSL(self.server, self.port) as server:
                        # Авторизация на сервере
                        server.login(self.username, self.password)
                        
                        # Отправка письма
                        server.sendmail(self.from_address, self.to_address, msg.as_string())

                        server.quit()