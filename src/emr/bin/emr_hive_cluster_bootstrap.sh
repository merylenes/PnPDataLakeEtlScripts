#!/bin/bash

function ssm_install( ) {

    ## Name: SSM Agent Installer Script
    ## Description: Installs SSM Agent on EMR cluster EC2 instances and update hosts file
    ##
    sudo yum install -y https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/linux_amd64/amazon-ssm-agent.rpm
    sudo status amazon-ssm-agent >>/tmp/ssm-status.log
    ## Update hosts file
    echo "\n ########### localhost mapping check ########### \n" > /tmp/localhost.log
    lhost=`sudo cat /etc/hosts | grep localhost | grep '127.0.0.1' | grep -v '^#'`
    v_ipaddr=`hostname --ip-address`
    lhostmapping=`sudo cat /etc/hosts | grep $v_ipaddr | grep -v '^#'`
    if [ -z "${lhostmapping}" ];
    then
        echo "\n ########### IP address to localhost mapping NOT defined in hosts files. add now ########### \n " >> /tmp/localhost.log
        sudo echo "${v_ipaddr} localhost" >>/etc/hosts
    else
        echo "\n IP address to localhost mapping already defined in hosts file \n" >> /tmp/localhost.log
    fi
    echo "\n ########### IP Address to localhost mapping check complete and below is the content ########### " >> /tmp/localhost.log
    sudo cat /etc/hosts >> /tmp/localhost.log
    
    echo "\n ########### Exit script ########### " >> /tmp/localhost.log
}

function bitbucket_priv_key( )  {
    # Add the bitbucket RSA private key to the cluster to get the repo
    echo "Add the bitbucket RSA private key to the cluster to get the repo"
 
    aws ssm get-parameter --name /bitbucket/ssh-key/id_rsa --with-decryption --query Parameter.Value --output text > ~/.ssh/id_rsa 
    chmod 400 ~/.ssh/id_rsa
 
    ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts
}

function python_libs ( ) {
    ## --- Upgrade pip
    PYTHON=$(which python3)

    ## --- Install python packages
    $PYTHON -m pip install --upgrade boto3 --user

    # sudo /usr/bin/pip3 uninstall -y numpy

    $PYTHON -m pip install numpy pyarrow s3fs awswrangler psycopg2-binary --user

    # Packages required by most customers
    $PYTHON -m pip install paramiko nltk scipy scikit-learn pandas --user
}

function hudi_libs ( ) {

   HDFS=/usr/lib/hadoop-hdfs/bin/hdfs
    # This is done as the hadoop user
   if [ -x $HDFS ]
   then
      $HDFS dfs -mkdir -p /apps/hudi/lib
      $HDFS dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
      $HDFS dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar
      $HDFS dfs -copyFromLocal /usr/lib/hudi/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar /apps/hudi/lib/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar
      $HDFS dfs -copyFromLocal /usr/lib/hadoop/lib/httpcore-4.4.11.jar /apps/hudi/lib/httpcore-4.4.11.jar
      $HDFS dfs -copyFromLocal /usr/lib/hadoop/lib/httpclient-4.5.9.jar /apps/hudi/lib/httpclient-4.5.9.jar
   elif [ -x /usr/bin/hadoop ]
   then
      /usr/bin/hadoop fs -mkdir -p /apps/hudi/lib 
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hudi/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar /apps/hudi/lib/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hadoop/lib/httpcore-4.4.11.jar /apps/hudi/lib/httpcore-4.4.11.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hadoop/lib/httpclient-4.5.9.jar /apps/hudi/lib/httpclient-4.5.9.jar
   else
      echo "Can't copy files to HDFS since can't find binaries to do it"
      echo "No /usr/bin/hadoop or /usr/bin/hdfs in the path"
   fi
}

#case "${STAGE}x" in
#   # Change this once the itrazo-data production branch moves to MASTER
#   "prodx" | "stagex" | "stagingx") BRANCH="develop";;
#   *) BRANCH="develop";;
#esac

set -x
#if [ $# -eq 0 ]
#    then
#        >&2 echo "No Arguments Passed"
#        exit 1
#fi

CUSTOMER='PicknPay'
ENV=${1}

#set listchars=eol:$,tab:>-,trail:~,extends:>,precedes:<
#set list
cat <<VimRc >~/.vimrc
set bg=dark
set ts=3
set et
VimRc

# Enable the epel packages:
echo "Installing EPEL from amazon-linux-extras"
sudo amazon-linux-extras install -y epel

# --- Install packages
sudo yum -y install figlet git python3-devel postgresql byobu cmake parallel

# --- Change the banner 
WHATAMI=`sudo cat /mnt/var/lib/info/extraInstanceData.json|jq -r '.instanceRole'`

HOST=$(hostname|sed '/^ip/!d;s/ip-\([^\.]*\).*/\1/')
WHAT=$(echo $WHATAMI|cut -c1)
cat <<BashRc >>~/.bashrc
# ~~~~ Added by bootstrap script ~~~~
export PS1='[ ${WHAT} \u@${HOST} \W ]\$ '
yarn node -list -all 2>/dev/null|sed '/Total Nodes\|ip-/!d;s/\(ip-[^:]*\).*/\t\1/'
export PATH=/root/.local/bin:$PATH
BashRc
source ~/.bashrc

python_libs

figlet -f script -r $CUSTOMER > motd
figlet -r "EMR    $WHATAMI" >> motd
sed -i "s/'/\"/g" motd
sed -i "s/^/echo '/;s/$/';/" motd

sudo mv /etc/profile.d/motd.sh /etc/profile.d/orig.motd.sh
sudo mv motd /etc/profile.d/motd.sh

# --- Get ParquetTools
mkdir ~/bin
wget -nv https://repo1.maven.org/maven2/org/apache/parquet/parquet-tools/1.10.0/parquet-tools-1.10.0.jar -P ~/bin/

# Get the MySQL driver
sudo wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar -P /usr/lib/spark/jars/
# Get the postgres driver
sudo wget https://repo1.maven.org/maven2/postgresql/postgresql/9.1-901.jdbc4/postgresql-9.1-901.jdbc4.jar  -P /usr/lib/spark/jars/

# Datalake prod scripts are in datalake/bin while cluster rotation (infrastructure scripts) in bin/xxxx

SCRIPTHOME=/home/hadoop/datalake/bin

echo "Copy prod Spark scripts to $SCRIPTHOME"
[ ! -d $SCRIPTHOME ] || mkdir -p $SCRIPTHOME

# Setting up prod INFRASTRUCTURE scripts
ssm_install

whoami
which hdfs
sudo find / -name bin\/hdfs
# Copy Hudi libraries to HDFS for later use
#hudi_libs
exit 0
