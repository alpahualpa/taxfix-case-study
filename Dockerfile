FROM python:3.10

WORKDIR /taxfix_task

COPY ./requirements.txt /taxfix_task/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /taxfix_task/requirements.txt

COPY ./core /taxfix_task/core

CMD ["python", "-u", "/taxfix_task/core/main.py"]