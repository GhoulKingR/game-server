FROM python:3.13.3-alpine

WORKDIR /code

COPY . .
RUN pip install -r requirements.txt
EXPOSE 8000

CMD ["python", "main.py"]
