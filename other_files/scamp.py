from datetime import timedelta

import pyscamp
import numpy as np
import time

start = time.time()

def rawcount(filename):
    f = open(filename, 'rb')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        lines += buf.count(b'\n')
        buf = read_f(buf_size)

    return lines


def scamp(time_series):
    time_series_len = rawcount('/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomwalk1M.txt')
    profile, index = pyscamp.selfjoin(time_series[:-1], 100)

    print(time_series[-1])
    print(time_series[time_series_len - 1])

    profile = profile.tolist()
    index = index.tolist()
    # res = list(zip(index, profile))
    return profile


if __name__ == '__main__':
    time_series = np.loadtxt('/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomwalk1M.txt',
                             dtype=np.float64)
    mp = scamp(time_series)

    # for item in mp:
    #     print(item)

end = time.time()
print(str(timedelta(seconds=end-start)))
# python other_files/scamp.py > other_files/output/scamp.txt
