from python:3.8

WORKDIR /usr/src/app

COPY scripts/* ./
RUN pip install --no-cache-dir -r requirements.txt
ARG GITHUB_SHA
ENV GITHUB_SHA=$GITHUB_SHA
#CMD [ "apt-get", "install", "telnet" ]
ENTRYPOINT [ "python", "./entrypoint.py"]