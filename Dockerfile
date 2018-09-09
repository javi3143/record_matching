FROM python:3.6
MAINTAINER Javier Ruiz "jruizperez@dundee.ac.uk"

COPY ./app /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "main.py" ]