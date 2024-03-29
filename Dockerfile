FROM python:3.10.9-alpine

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
 
COPY ./app /code/app

EXPOSE 80/tcp
 
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

