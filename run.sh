#!/usr/bin/env bash

op=$1
instance=$2
if [ -z "$op" ] || [ -z "$instance" ]
then
  echo 'Usage:$0 start|stop|rename instance'
  exit 1
fi

# 监控数据库实例在mysql dbbase进行rename相关sql操作
function excute(){
    file=$1
    sql=$2
    host=`grep "^instance_host" ${file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    port=`grep "^instance_port" ${file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    user=`grep "^instance_user" ${file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    password=`grep "^instance_password" ${file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    db_base=mysql
    /usr/bin/mysql -h ${host} -P ${port} -u${user} -p${password} -D${db_base} -e "$sql"
}

# 尝试启动监控实例进程
function start(){
   name=$1
   cfg1=$2
   cfg2=$3
   num=`ps -ef|grep "monitor_main"|grep "${name}"|grep -v grep|wc -l`
    if [ "$num" == 0 ]
    then
        nohup /usr/bin/python3 monitor_main.py ${cfg1} ${cfg2} &
    else
        echo "${name} has be running, please check!!!"
    fi
}

# kill实例监控进程
function stop(){
    name=$1
    num=`ps -ef|grep "monitor_main"|grep "${name}"|grep -v grep|wc -l`
    if [ "$num" > 0 ]
    then
        ps -ef|grep "monitor_main"|grep "${name}"|grep -v grep|awk '{print $2}'|xargs kill
    fi
}

# 尝试创建配置文件软连接
function link(){
    source=$1
    d_link=$2
    if [ ! -e ${d_link} ]
    then
        # 为了便于区分实例, 用于kill
        ln -s ${source} ${d_link}
    fi
}

cfg_path=/data/mmDev/mysql_monitor/conf
cfg_file=${cfg_path}/monitor.conf
log_conf_file=${cfg_path}/logging.conf


if [ "$op" == "start" ]
then
    cfg_link_file=${cfg_path}/${instance}_monitor.conf
    link ${cfg_file} ${cfg_link_file}
    start ${instance} ${cfg_link_file} ${log_conf_file}
elif [ "$op" == "stop" ]
then
    stop ${instance}
elif [ "$op" == "rename" ]
then
    rename_cfg_file=${cfg_path}/rename.conf
    record_file=`grep "^record_file" ${cfg_file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    rename_record_file=`grep "^record_file" ${rename_cfg_file}|awk -F"=" '{print $2}'|sed 's/ //g'`
    # 停掉当前实例,保存状态文件
    stop ${instance}
    mv ${record_file} ${rename_record_file}
    # rename mysql相关表
    sql="drop table if exists mysql.general_log_drop;"
    excute ${sql}
    sql="drop table if exists mysql.general_log_tmp;"
    excute ${sql}
    sql="create table mysql.general_log_tmp like mysql.general_log;"
    excute ${sql}
    sql="rename table mysql.general_log to mysql.general_log_drop, mysql.general_log_tmp to mysql.general_log;"
    excute ${sql}
    # 启动实例
    start ${instance} ${cfg_link_file} ${log_conf_file}
    # 启动遗留日志信息处理
    nohup /usr/bin/python3 monitor_main.py ${rename_cfg_file} ${log_conf_file} &
fi