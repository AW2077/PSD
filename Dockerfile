FROM flink:1.19

# Instalacja Pythona i pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Instalacja PyFlinka (wersja zgodna z obrazem bazowym)
RUN pip3 install apache-flink==1.19.0