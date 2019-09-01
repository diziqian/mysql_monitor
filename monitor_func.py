#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
from datetime import datetime
import logging
import traceback
from monitor_const import *


# 程序运行入口, 从上次断电处读取general_log，并存到目标数据库上
def run_process(config):
    global g_record_file
    global g_line

    g_record_file = config.record_file
    # record_line 为当前已经读取的条数
    record_line = 0
    if os.path.exists(g_record_file):
        with open(g_record_file) as fp:
            str_line = fp.read()
            if str_line.isdigit():
                record_line = int(str_line)
    g_line = record_line
    logging.debug('deal log from {0} line'.format(record_line))

    # i_count 为当前读的行数
    i_count = 0

    mdb = config.get_mdb()

    # thread_id + server_id 对应的dbname
    db_dic = {}
    sqls = []
    with open(config.task_file, encoding='utf-8') as fp:
        while True:
            lines = fp.readlines()
            if not lines:
                if config.auto_exit:
                    if len(sqls) > 0:
                        try:
                            mdb.execute(sqls)
                            mdb.commit()
                        except Exception as e:
                            mdb.rollback()
                            logging.error(traceback.format_exc(e))
                    break
                else:
                    time.sleep(1)
                    continue


            for i in range(len(lines)):
                if i_count < record_line:
                    i_count += 1
                    continue
                line = lines[i].strip().lower()
                sql = read_line(line, db_dic, config.instance)
                if sql != "":
                    sqls.append(sql)

                i_count += 1

                if len(sqls) >= COMMIT_MODULO:
                    try:
                        mdb.execute(sqls)
                        mdb.commit()
                        g_line = i_count
                        sqls = []
                    except Exception as e:
                        mdb.rollback()
                        logging.error(traceback.format_exc(e))

                if i_count % LOG_MODULO == 0:
                    write_record()



# 对set、show、commit、/*！和 field list等无关语句进行了忽略；由于该语句可能会引号，故位置为0或1
def ignore_sentence(line_arr):
    dic_sentence = {'set': True, 'show': True, 'explain ': True, 'field': True, 'commit': True}

    words = line_arr[ARGUMENT_SEQ].split()
    if len(words) > 0 and words[0] in dic_sentence:
        return True
    elif line_arr[ARGUMENT_SEQ].find("/*!") == 0:
        return True

    words = line_arr[COMMAND_TYPE_SEQ].split()
    if len(words) > 0 and words[0] in dic_sentence:
        return True

    return False


# 读取一行并处理，处理方法为：1）不符合的忽略；2）符合要求形成SQL语句
def read_line(line, db_dic, instance):
    sql = ""
    if len(line) == 0:
        return sql
    line_arr = line.split(',')
    if len(line) <= ARGUMENT_SEQ:
        return sql
    line_arr[EVENT_TIME_SEQ] = line_arr[EVENT_TIME_SEQ].strip('"\`\r\n ')
    line_arr[USER_HOST_SEQ] = line_arr[USER_HOST_SEQ].strip('"\`\r\n ')
    line_arr[THREAD_ID_SEQ] = line_arr[THREAD_ID_SEQ].strip('"\`\r\n ')
    line_arr[SERVER_ID_SEQ] = line_arr[SERVER_ID_SEQ].strip('"\`\r\n ')
    line_arr[COMMAND_TYPE_SEQ] = line_arr[COMMAND_TYPE_SEQ].strip('"\`\r\n ')
    line_arr[ARGUMENT_SEQ] = ','.join(line_arr[ARGUMENT_SEQ:])
    line_arr[ARGUMENT_SEQ] = line_arr[ARGUMENT_SEQ].strip('"\`\r\n ').replace('`', '').replace('"', '').replace("'", '')
    line_arr = line_arr[:ARGUMENT_SEQ + 1]

    if ignore_sentence(line_arr):
        return sql

    make_db_dic(line_arr, db_dic)
    key_id = '{0}_{1}'.format(line_arr[THREAD_ID_SEQ], line_arr[SERVER_ID_SEQ])
    if key_id in db_dic:
        line_arr.append(db_dic[key_id][1])
    else:
        line_arr.append("")

    sql = generate_sql(line_arr, instance)

    return sql


# 由特殊语句或命令获得DB名字
def make_db_dic(line_arr, db_dic):
    # init语句找DB
    cmd_type = line_arr[COMMAND_TYPE_SEQ]
    str_argu = line_arr[ARGUMENT_SEQ]
    if cmd_type.find('init ') == 0:
        db_name = str_argu
        add_to_db_dic(db_name, line_arr, db_dic)
    elif cmd_type.find('connect') == 0:
        # connect语句找DB
        connect_line = str_argu
        conn_arr = connect_line.split()
        if len(conn_arr) > 4 and conn_arr[1] == 'on':
            db_name = conn_arr[2]
            add_to_db_dic(db_name, line_arr, db_dic)
    elif str_argu.find('use ') == 0:
        # use database语句找数据库
        db_name = str_argu[3:].strip('"\`\r\n ')
        add_to_db_dic(db_name, line_arr, db_dic)


# 由字典获得DB
def add_to_db_dic(db_name, line_arr, db_dic):
    key_id = '{0}_{1}'.format(line_arr[THREAD_ID_SEQ], line_arr[SERVER_ID_SEQ])
    time_str = line_arr[EVENT_TIME_SEQ]
    datetime_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")

    if key_id in db_dic:
        lst_value = db_dic[key_id]
        # 以最近的数据库切换为准
        if lst_value[0] < datetime_obj:
            db_dic[key_id] = [datetime_obj, db_name]
    else:
        db_dic[key_id] = [datetime_obj, db_name]


# 拼接SQL语句，然后存目标数据库里
def generate_sql(line_arr, instance):
    sql = ""
    user_arr = line_arr[USER_HOST_SEQ].split('@')
    if len(user_arr) >= 2:
        user_lst = user_arr[0].strip('" ')
        if len(user_lst) <= 0:
            logging.warning('unexpect user format {0}'.format(line_arr[USER_HOST_SEQ]))
        items = user_lst.split('[')
        if len(items[0]) == 0:
            user = items[1].strip('] ')
        else:
            user = items[0].strip()

        user_host = user_arr[1].strip('"\`[] ')
        if user_host.find('[') >= 0:
            user_host = user_host.split('[')[0]

        sql = """insert into sql_monitor(user, db_host, db, argument, command_type, thread_id, server_id,
                login_host, event_time) values ('{0}', '{1}', '{2}', '{3}', '{4}', {5}, {6}, '{7}', '{8}')
            """.format(user, instance, line_arr[DB_NAME_SEQ], line_arr[ARGUMENT_SEQ], line_arr[COMMAND_TYPE_SEQ],
                       line_arr[THREAD_ID_SEQ], line_arr[SERVER_ID_SEQ], user_host, line_arr[EVENT_TIME_SEQ])

    return sql


# 结束信号响应
def term_sig_handler(signum, frame):
    logging.warning('get term singal: {0}, frame {1}'.format(signum, frame))
    write_record()
    sys.exit()


# 关闭数据库
def close_db(db_conn, cursor):
    cursor.close()
    db_conn.close()


# 把当前写入行数记录进外部文件里，方便下次程序对generak_log正确读取,防止程序被意外杀死用
def write_record():
    if g_line != 0 and len(g_record_file) != 0:
        with open(g_record_file, 'w', encoding='utf-8') as fp:
            fp.write('{0}'.format(g_line))
