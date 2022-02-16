# Build

FROM openjdk:11
COPY --from=python:3.8 / /
WORKDIR /source

COPY ./ ./

RUN pip install -r requirements.txt

# App
CMD [ "streamlit", "run", "app.py"]
