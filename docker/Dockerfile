FROM postgres:9.3.19

ENV PG_VERSION 9.3
ENV USER tester
ENV PASS tester

# install 
RUN apt-get update && apt-get install -y netcat build-essential apt-utils

# install Ancaonda 3.6
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apt-get update --fix-missing && apt-get install -y wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
    git mercurial subversion

RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget --quiet https://repo.continuum.io/archive/Anaconda3-5.0.0-Linux-x86_64.sh -O ~/anaconda.sh && \
    /bin/bash ~/anaconda.sh -b -p /opt/conda && \
    rm ~/anaconda.sh
ENV PATH /opt/conda/bin:$PATH
RUN conda create -n hk3 python=3.6

ENTRYPOINT [ “/bin/bash”, “-c” ]
CMD [ "source activate hk3" ]