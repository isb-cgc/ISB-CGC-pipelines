FROM ubuntu:14.04
ENV debian_frontent NONINTERACTIVE
RUN apt-get -y update && apt-get -y install build-essential python2.7 python-dev wget && apt-get -y clean
RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py && rm get-pip.py
RUN pip install --upgrade google-api-python-client && pip install --upgrade WebOb && pip install --upgrade Paste && pip install --upgrade webapp2
COPY ISB-CGC-pipelines/ /usr/bin/
RUN ln -s /usr/bin/ISB-CGC-pipelines/server/docker/pipelineServer.py /usr/bin/pipelineServer.py && chmod 700 /usr/bin/pipelineServer.py
ENV PYTHONPATH /usr/local/lib/python2.7:/usr/local/ISB-CGC-pipelines
EXPOSE 8080
CMD ["python", "pipelineServer.py"]

# NOTES:
# - Need to mount the default service account into the container at runtime so the webapp can make requests to google apis

