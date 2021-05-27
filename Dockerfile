FROM ubuntu:18.04

RUN apt-get update && \
    apt --assume-yes install  python3.8 && \
    apt --assume-yes install python3-pip && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.8 10 && \
    python -m pip install pip && \
    python -m pip install pandas requests boto3



 
copy api_update.py .