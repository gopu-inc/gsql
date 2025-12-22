FROM python:3.9  # Version complète, pas 'slim'

WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install -e .

ENTRYPOINT ["gsql"]
CMD ["--help"]
