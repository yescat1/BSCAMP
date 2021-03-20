import matplotlib.pyplot as plt
import datetime


def get_secs(hlist):
    for i in range(len(hlist)):
        h, m, s = hlist[i].split(':')
        hlist[i] = int(datetime.timedelta(hours=int(h), minutes=int(m), seconds=int(s)).total_seconds())
    return hlist

ref = ['32k', '64k', '128k', '256k', '512k', '1M', '2M']
scamp = ['0:0:0', '0:0:1', '0:0:4', '0:0:16', '0:1:2', '0:3:14', '0:13:59']
bscamp_cpu = ['0:0:34', '0:0:41', '0:1:03', '0:2:11', '0:7:30', '3:53:29', '2:31:58']
bscamp = ['0:6:43', '0:7:27', '0:8:13', '0:11:11', '0:24:23', '1:11:58', '1:25:32']

tile_size = ['10000', '50000', '100000', '200000']
tile_runtime = ['1:11:58', '0:38:36', '0:35:17', '0:32:35']

scamp = get_secs(scamp)
bscamp_cpu = get_secs(bscamp_cpu)
bscamp = get_secs(bscamp)
tile_runtime = get_secs(tile_runtime)

print(scamp)
print(bscamp_cpu)
print(tile_runtime)

plt.figure(figsize=(7,2))
# plt.plot(ref, scamp, label="scamp")
# plt.plot(ref, bscamp, label="bscamp")
# plt.plot(ref, bscamp_cpu, label="bscamp_cpu")

plt.plot(tile_size, tile_runtime, label="BSCAMP Dataflow runtime")
plt.xlabel("Tile size")
plt.ylabel("Runtime (seconds)")
plt.legend(loc="upper right")
plt.show()