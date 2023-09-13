FROM python:3.9.1



WORKDIR /app
COPY pipeline.py pipeline-A.py
#COPY all_seasons.csv source.csv

RUN pip install pandas
RUN pip install numpy 

ENTRYPOINT ["python","pipeline-A.py"]