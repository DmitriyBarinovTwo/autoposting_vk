FROM python:3.10.5

RUN mkdir -p /usr/src/app/
WORKDIR /usr/src/app/

COPY . /usr/src/app/
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV TZ Europe/Moscow

CMD ["python","main.py"]