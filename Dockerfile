FROM python:3.9

# Instala as dependências necessárias
RUN apt-get update && apt-get install -y \
    postgresql-client

# Instala as bibliotecas Python necessárias
RUN pip install psycopg2 pymongo

# Copia o script Python para o contêiner
COPY service.py /app/service.py
WORKDIR /app

# Define o comando padrão para iniciar o serviço
CMD ["python", "service.py"]
