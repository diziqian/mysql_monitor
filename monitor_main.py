#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import signal
import sys
import getopt
import logging.config
from monitor_config import MdConfig
from monitor_func import term_sig_handler, run_process


def main():
    opts, args = getopt.getopt(sys.argv, "h", ["help"])
    if len(args) != 3:
        print('Usage {0} config_file log_conf'.format(sys.argv[0]))
        sys.exit(-1)
    config_file = args[1]
    log_conf = args[2]

    config = MdConfig(config_file)
    logging.config.fileConfig(log_conf)

    signal.signal(signal.SIGTERM, term_sig_handler)
    signal.signal(signal.SIGINT, term_sig_handler)
    signal.signal(signal.SIGQUIT, term_sig_handler)

    run_process(config)


if __name__ == '__main__':
    main()
