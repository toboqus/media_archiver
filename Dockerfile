FROM alpine:latest
RUN apk --update add python3 python3-dev py3-pip zlib-dev gcc libgcc musl-dev jpeg-dev

COPY . /app

WORKDIR /app

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3"]

CMD ["main.py"]