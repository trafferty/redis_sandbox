import redis

# some redis utils
#
def zminmaxscore(r, key):
    try:
        min = r.zrangebyscore(key, '-inf', '+inf', withscores=True, start=1, num=1)[0][1]
        max = r.zrevrangebyscore(key, '+inf', '-inf', withscores=True, start=1, num=1)[0][1]
        return (min, max)
    except Exception as e:
        print( str(e) )
        return tuple()




# utils specific to DIF
#
def parseExpRoot(hash_in):
    # expRoot: "ExperimentID":run_num:"HeadSerialNumber":"NozzleSize":"MonomerID"
    return ':'.join(hash_in.split(':')[0:5])


def getNozzlePhase(nozzle_num):
    return ["B", "C", "A"][(nozzle_num%3)]


