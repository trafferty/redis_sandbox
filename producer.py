#!/usr/bin/env python

import redis
import signal
import json
import datetime as dt
import logging, logging.config
import coloredlogs
import time
from random import random, randint

if __name__ == '__main__':
    done = False
    def sigint_handler(signal, frame):
        global done
        print( "\nShutting down...")
        done = True
    signal.signal(signal.SIGINT, sigint_handler)

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("producer")
    fmt = "%(asctime)s.%(msecs)03d (%(name)10s) [%(levelname)s]: %(message)s"
    coloredlogs.install(level='DEBUG', fmt=fmt, field_styles=coloredlogs.DEFAULT_FIELD_STYLES, level_styles=coloredlogs.DEFAULT_LEVEL_STYLES)

    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    exp_vars = json.loads('''
    {
        "Location": "DIF Station",
        "HeadSerialNumber": "SNX1227354",
        "ExperimentID": "2017-09-22_11.37.25",
        "double_shutter": "true",
        "Camera": "Andor",
        "double_shutter_delay_us": "4.000000",
        "DIF_Version": "0.2.0",
        "MonomerID": "FF031A",
        "NozzleSize": "14",
        "Experiment Type": "DispenseOpt",
        "Magnification": "20x",
        "ImageFileRoot": "/media/DIF_DATA/images/FF032A/14um/1227354/row1/2017-09-22_11.37.25/",
        "git-sha": "96ec1353697c2452e06b3cb6fe30d97156ce9083",
        "git-branch": "dif_branch"
    }''')

    loop_vars = json.loads('''
    {
        "Sample Clock": "110.000000",
        "NozzleID": "2.000000",
        "BitmapFile": "recipe/g2x4_full_head_dif.bmp",
        "ImageFile": "DispenserOpt_00005_2017-09-22_11.37.33.png",
        "Nozzle": "2.000000",
        "LoopID": "5",
        "Global Voltage": "18.700000",
        "PD_delay_us": "290.000000",
        "Waveform": "F661_DS.txt",
        "Row": "1.000000",
        "Phase": "A"
    }''')

    exp_id = dt.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    exp_vars["ExperimentID"] = exp_id

    #exp_root = "%s:%s:%d" % (dt.datetime.now().strftime("%Y-%m-%d"), 'SN987234', 1)
    exp_root = "%s:%d:%s:%s:%s" % (exp_vars["ExperimentID"], 1, exp_vars["HeadSerialNumber"],exp_vars["NozzleSize"], exp_vars["MonomerID"])

    r.hmset(exp_root, exp_vars)
    r.sadd('experiments_started', exp_root)

    num_loops = 1000
    for loop_cnt in range(1, num_loops+1):
        loop_vars["LoopID"] = str(loop_cnt)
        loop_vars["Sample Clock"] = '%3.3f' % (randint(0, 30)+random())
        loop_vars["Global Voltage"] = '%3.3f' % (randint(16, 25)+random())
        loop_vars["PD_delay_us"] = '%d' % (randint(0, 10)*10)
        loop_vars["ImageFile"] = 'DispenserOpt_%05d_%s.png' % (loop_cnt, dt.datetime.now().strftime("%Y-%m-%d-%H.%M.%S"))
        loop_vars["Phase"] = '%s' % (['A', 'B', 'C'][loop_cnt%3])
        loop_vars["Nozzle"] = '%d' % (randint(0,499))
        loop_vars["Row"] = '%d' % (randint(0,1))
        loop_vars["NozzleID"] = '%d' % ( (int(loop_vars["Row"])*500) + int(loop_vars["Nozzle"]) )

        loop_hash = "%s:vars:%d" % (exp_root, loop_cnt)
        r.hmset(loop_hash, loop_vars)
        r.rpush('new_image_data', loop_hash)

        logger.info("Added loop: %s.  len=%d" % (loop_hash, r.llen('new_image_data')))
        if done: break
        time.sleep(2)

    r.sadd('experiments_completed', exp_root)
