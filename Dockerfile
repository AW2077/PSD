FROM flink:1.19

# Instalacja Pythona i pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev build-essential g++ gfortran libbsd-dev unzip && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir --force-reinstall \
    river==0.15.0 \
    pymongo \
    pandas \
    ruamel.yaml \
    protobuf==3.20.3 \
    pytest \
    apache-beam==2.48.0 \
    pemja==0.3.0 \
    py4j \
    grpcio \
    avro-python3

USER root
RUN unzip /opt/flink/opt/python/pyflink.zip -d /usr/local/lib/python3.10/dist-packages/
RUN chmod -R +x /usr/local/lib/python3.10/dist-packages/pyflink/bin/
USER flink