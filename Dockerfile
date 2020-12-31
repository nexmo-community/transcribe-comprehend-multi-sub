FROM python:3.8.5
WORKDIR /usr/src/transcribe-comprehend-multi-sub
COPY .env.example .env
COPY . .
RUN pip install --upgrade -r requirements.txt
CMD ["python", "transcribe-comprehend-multi-sub.py"]
EXPOSE 5000