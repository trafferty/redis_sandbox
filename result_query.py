#!/usr/bin/env python

import redis
import signal
import json
import datetime as dt
import logging, logging.config
import coloredlogs
import time
from random import random, randint

def process_img(image_path):
    time.sleep(0.5)

if __name__ == '__main__':
    done = False
    def sigint_handler(signal, frame):
        global done
        print( "\nShutting down...")
        done = True
    signal.signal(signal.SIGINT, sigint_handler)

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("consumer")
    fmt = "%(asctime)s.%(msecs)03d (%(name)10s) [%(levelname)s]: %(message)s"
    coloredlogs.install(level='DEBUG', fmt=fmt, field_styles=coloredlogs.DEFAULT_FIELD_STYLES, level_styles=coloredlogs.DEFAULT_LEVEL_STYLES)

    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)


    results = r.smembers('results')




    while not done:
        loop_hash = r.lpop('new_image_data')
        if loop_hash:
            print(loop_hash)
            loop_vars = r.hgetall(loop_hash)
            exp_root = ':'.join(loop_hash.split(':')[0:4])
            exp_vars = r.hgetall(exp_root)
            # merge the two dicts (p3.5+)
            all_vars = {**loop_vars, **exp_vars}

            image_path = "%s%s" % (all_vars["ImageFileRoot"], all_vars["ImageFile"])
            logger.info("Processing image: %s" % (image_path))
            process_img(image_path)
            logger.info("Processing complete. queue len=%d" % (r.llen('new_image_data')))

            result_hash = "%s:result:%s:%s" % (exp_root, all_vars["Row"], all_vars["Nozzle"])

            # first, see if we already have result data for this row, nozzle
            results = r.hgetall(result_hash)
            if results:
                results["volume"] = "%s,%s" % (results["volume"], '%.2f' % (randint(0, 99)/100) )
                results["vel"] = "%s,%s" % (results["vel"], '%.2f' % (randint(30, 70)/10) )
            else:
                result_dict = {}
                results["volume"] = "%s" % ('%.2f' % (randint(0, 99)/100) )
                results["vel"] = "%s" % ('%.2f' % (randint(30, 70)/10) )

            r.hmset(result_hash, results)
            r.sadd('results', result_hash)

        else:
            logger.warn("Nothing to process.")
            time.sleep(2)
