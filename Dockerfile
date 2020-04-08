FROM centos:7

# add yum repos
RUN set -e \
			&& yum install -y wget \
			&& cd /etc/yum.repos.d \
			&& wget http://yum.oracle.com/public-yum-ol7.repo \
			&& wget http://yum.oracle.com/RPM-GPG-KEY-oracle-ol7 \
			&& rpm --import RPM-GPG-KEY-oracle-ol7 \
			&& yum install -y yum-utils \
			&& yum-config-manager --enable ol7_oracle_instantclient \
			&& yum --enablerepo=extras install -y epel-release

# install unixodbc and mssql drivers
RUN set -e \
			&& yum update -y \
			&& yum install -y git libcurl-devel libxml2-devel openssl-devel R-devel unixODBC-devel \
			&& curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo \
			&& ACCEPT_EULA=Y yum install -y msodbcsql17 mssql-tools

# install oracle instant client 18.3 binaries
RUN yum -y install oracle-instantclient18.3-basic oracle-instantclient18.3-devel oracle-instantclient18.3-odbc mysql-devel postgresql-devel
RUN echo /usr/lib/oracle/18.3/client64/lib/ | tee /etc/ld.so.conf.d/oracle.conf && chmod o+r /etc/ld.so.conf.d/oracle.conf
RUN ldconfig
RUN echo 'export ORACLE_HOME=/usr/lib/oracle/18.3/client64' | tee /etc/profile.d/oracle.sh && chmod o+r /etc/profile.d/oracle.sh
RUN . /etc/profile.d/oracle.sh

# install clir
RUN set -e \
      && cd \
			&& git clone https://github.com/dceoy/clir.git ~/.clir \
			&& ~/.clir/install_clir.sh --root \
			&& echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
			&& echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
			#&& echo 'export R_LIBS_USER="$HOME/.clir/r/library"' >> ~/.bash_profile \
			&& source ~/.bashrc \
			&& mkdir -p /usr/share/doc/R-3.6.0/html \
			&& cp /usr/lib64/R/library/stats/html/R.css /usr/share/doc/R-3.6.0/html/

# install R packages
RUN set -e \
			&& source ~/.bash_profile \
      && clir update \
      && clir install --devt=cran dbplyr doParallel foreach gridExtra rmarkdown tidyverse odbc dotenv RPostgreSQL RMySQL\
      && clir validate dbplyr doParallel foreach gridExtra rmarkdown tidyverse odbc dotenv RPostgreSQL RMySQL \
      && clir install --devt=github datawookie/emayili \
      && clir validate emayili

# clone proteus and add to container
ADD .env /app/.env
ADD *.R /app/
ADD /inst/ /app/inst
ADD odbc.ini /etc/odbc.ini
ADD odbcinst.ini /etc/odbcinst.ini

CMD R -e "source('/app/04-execute.R')"
