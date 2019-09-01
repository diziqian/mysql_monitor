#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import logging
import logging.config
import unittest

from monitor_mysql import DbMysql


class MdConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)

        # instance
        self.instance_host = cf.get('public', 'instance_host')
        self.instance_port = cf.get('public', 'instance_port')
        self.instance = '{0}:{1}'.format(self.instance_host, self.instance_port)
        self.auto_exit = cf.getboolean('public', 'auto_exit')

        # file路径
        self.task_file = cf.get('file', 'task_file')
        self.record_file = cf.get('file', 'record_file')

        # mysql config
        self.my_host = cf.get('mysql', 'host')
        self.my_port = cf.getint('mysql', 'port')
        self.my_user = cf.get('mysql', 'user')
        self.my_password = cf.get('mysql', 'password')
        self.my_db = cf.get('mysql', 'db')
        self.mdb = DbMysql(self.my_host, self.my_port, self.my_user, self.my_password, self.my_db)
        logging.info('mysql host[{0}] port[{1}]'.format(self.my_host, self.my_port))

    def get_mdb(self):
        return self.mdb


class TestMdConfig(unittest.TestCase):
    def test_mdb(self):
        conf_file = './conf/monitor.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)

        md_config = MdConfig(conf_file)
        mdb = md_config.get_mdb()
        print(mdb.query('select 1 from dual'))


if __name__ == '__main__':
    unittest.main()
