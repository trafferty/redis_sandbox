#!/usr/bin/env python

import redis
import signal
import json
import datetime as dt
import logging, logging.config
import coloredlogs
import time
from random import random, randint

# local:
import utils

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

    completed_list = r.smembers('experiments_completed')

    for exp_root in completed_list:




    while not done:
        #loop_hash = r.lpop('new_image_data')
        l, loop_hash = r.blpop('new_image_data', 2)
        if loop_hash:
            print(loop_hash)
            loop_vars = r.hgetall(loop_hash)
            exp_root = utils.parseExpRoot(loop_hash)
            exp_vars = r.hgetall(exp_root)
            # merge the two dicts (p3.5+)
            all_vars = {**loop_vars, **exp_vars}

            image_path = "%s%s" % (all_vars["ImageFileRoot"], all_vars["ImageFile"])
            logger.info("Processing image: %s" % (image_path))
            process_img(image_path)
            logger.info("Processing complete. queue len=%d" % (r.llen('new_image_data')))

            result_hash = "%s:res:%s:%s:%s" % (exp_root, all_vars["Row"], all_vars["Nozzle"], all_vars["Loop_Count"])

            results = {}

            # volume in femptaliters
            volume = randint(400,1000) / 1000.0
            # velocity in mm/s
            vel    = ((randint(3, 6) + random())

            results["volume"] = str(volume)
            results["vel"] = str(vel)

            results["Row"] = all_vars["Row"]
            results["Nozzle"] = all_vars["Nozzle"]

            results["Sample Clock"] = all_vars["Sample Clock"]
            results["BitmapFile"] = all_vars["BitmapFile"]
            results["Global Voltage"] = all_vars["Global Voltage"]
            results["PD_delay_us"] = all_vars["PD_delay_us"]
            results["Waveform"] = all_vars["Waveform"]
            results["Phase"] = all_vars["Phase"]

            r.hmset(result_hash, results)
            r.sadd('results', result_hash)

            vel_hash = "%s:res_vel_set" % (exp_root)
            r.zadd(vel_hash, vel, result_hash)
            volume_hash = "%s:res_vol_set" % (exp_root)
            r.zadd(volume_hash, volume, result_hash)


        else:
            logger.warn("Nothing to process.")
            #time.sleep(2)
