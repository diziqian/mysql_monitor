#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 定义常量数字
EVENT_TIME_SEQ = 0
USER_HOST_SEQ = 1
THREAD_ID_SEQ = 2
SERVER_ID_SEQ = 3
COMMAND_TYPE_SEQ = 4
ARGUMENT_SEQ = 5
OTHER_SEQ = 6
DB_NAME_SEQ = 6
COMMIT_MODULO = 256
LOG_MODULO = 1024

# 把当前操作记录到外部文件，用于防止程序崩溃
g_record_file = ''
g_line = 0
