import time, os


def re_exe(cmd, inc = 60):
    while True:
        os.system(cmd)
        time.sleep(inc)


print("ready to excute scrapt...")
re_exe("python BITMAX_jackendoff.py", 300)


