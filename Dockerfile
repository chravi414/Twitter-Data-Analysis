FROM python:3.7
ENV PYTHONUNBUFFERED 1

RUN curl -L -O "https://oraclemirror.np.gy/jdk8/jdk-8u251-linux-x64.tar.gz"
RUN mkdir /usr/local/java/
RUN tar -xvzf jdk-8u251-linux-x64.tar.gz -C /usr/local/java/
RUN rm jdk-8u251-linux-x64.tar.gz

RUN update-alternatives --install "/usr/bin/java" "java" "/usr/local/java/jdk1.8.0_251/bin/java" 1
RUN update-alternatives --install "/usr/bin/javac" "javac" "/usr/local/java/jdk1.8.0_251/bin/javac" 1
RUN update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/local/java/jdk1.8.0_251/bin/javaws" 1
ENV JAVA_HOME="/usr/local/java/jdk1.8.0_251"

RUN curl -L -O "https://downloads.apache.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz"

RUN mkdir -p /usr/local/spark-2.4.5
RUN tar -zxf spark-2.4.5-bin-hadoop2.7.tgz -C /usr/local/spark-2.4.5/
RUN rm spark-2.4.5-bin-hadoop2.7.tgz
RUN update-alternatives --install "/usr/sbin/start-master" "start-master" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/start-master.sh" 1
RUN update-alternatives --install "/usr/sbin/start-slave" "start-slave" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/start-slave.sh" 1
RUN update-alternatives --install "/usr/sbin/start-slaves" "start-slaves" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/start-slaves.sh" 1
RUN update-alternatives --install "/usr/sbin/start-all" "start-all" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/start-all.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-all" "stop-all" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/stop-all.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-master" "stop-master" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/stop-master.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-slaves" "stop-slaves" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/stop-slaves.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-slave" "stop-slave" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/stop-slave.sh" 1
RUN update-alternatives --install "/usr/sbin/spark-daemon.sh" "spark-daemon.sh" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/spark-daemon.sh" 1
RUN update-alternatives --install "/usr/sbin/spark-config.sh" "spark-config.sh" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/sbin/spark-config.sh" 1
RUN update-alternatives --install "/usr/bin/spark-shell" "spark-shell" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/spark-shell" 1
RUN update-alternatives --install "/usr/bin/spark-class" "spark-class" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/spark-class" 1
RUN update-alternatives --install "/usr/bin/spark-sql" "spark-sql" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/spark-sql" 1
RUN update-alternatives --install "/usr/bin/spark-submit" "spark-submit" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/spark-submit" 1
RUN update-alternatives --install "/usr/bin/pyspark" "pyspark" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/pyspark" 1
RUN update-alternatives --install "/usr/bin/load-spark-env.sh" "load-spark-env.sh" "/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7/bin/load-spark-env.sh" 1
ENV SPARK_HOME="/usr/local/spark-2.4.5/spark-2.4.5-bin-hadoop2.7"

RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]