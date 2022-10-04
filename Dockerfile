FROM python:3.9

WORKDIR /code

COPY main.py .

COPY requirements.txt .

COPY snowCred.json .

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

CMD ["python", "./main.py"]