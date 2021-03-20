

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


if __name__ == '__main__':
    lines = rawcount('/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomwalk512K.txt')
    print(lines)
