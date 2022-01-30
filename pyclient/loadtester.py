import time, sys
from operator import itemgetter
import client
from models import action, profile

# --------- CONSTANTS -----------

ACTION = 0
PROFILE_GET = 1
PROFILE_SET = 2

actionLogFname = "tests/actionLog"
if len(sys.argv) >= 2 and sys.argv[1] == "-s":
    actionLogFname += "Small"
getProfileFname = "tests/profileGets"
setProfileFname = "tests/profileSets"

# ---------- LOADING ------------


def lineParser(l):
    l = l.rstrip().split(" ", 1)

    t = int(l[0])
    b = bytes(map(int, l[1].split(" ")))

    return (t, b)


c = client.Client()

events = []

totalActionLogs = 0
totalProfileGets = 0
totalProfileSets = 0

with open(actionLogFname) as f:
    lines = f.readlines()
    for line in lines:
        (t, b) = lineParser(line)

        a = action.Action()
        a.ParseFromString(b)

        events.append([t, ACTION, a])
        totalActionLogs += 1

with open(getProfileFname) as f:
    lines = f.readlines()
    for line in lines:
        (t, b) = lineParser(line)

        p = profile.ProfileItem()
        p.ParseFromString(b)

        events.append([t, PROFILE_GET, p])
        totalProfileGets += 1

with open(setProfileFname) as f:
    lines = f.readlines()
    for line in lines:
        (t, b) = lineParser(line)

        p = profile.ProfileItem()
        p.ParseFromString(b)

        events.append([t, PROFILE_SET, p])
        totalProfileSets += 1

events.sort(key=itemgetter(0))

# ----------- MAIN --------------

t = time.perf_counter
t0 = t()

i = 0

actionLogTime = 0.0
profileGetTime = 0.0
profileSetTime = 0.0

numActionLogs = 0
numProfileGets = 0
numProfileSets = 0

while i != len(events) and i < 20:
    while (t() - t0) * 1e6 < events[i][0]:
        pass

    if events[i][1] == ACTION:
        tx = t()
        c.log(events[i][2])
        actionLogTime += t() - tx
        numActionLogs += 1
    elif events[i][1] == PROFILE_GET:
        tx = t()
        c.get_profile(events[i][2])
        profileGetTime += t() - tx
        numProfileGets += 1
    else:
        tx = t()
        c.set_profile(events[i][2])
        profileSetTime += t() - tx
        numProfileSets += 1

    i += 1

t1 = t()

print("Total time taken:", t1 - t0)
print("Time taken for action logs:", actionLogTime)
print("Time taken for profile gets:", profileGetTime)
print("Time taken for profile sets:", profileSetTime)
print("Remaining time:", t1 - t0 - actionLogTime - profileSetTime - profileGetTime)
if numActionLogs > 0:
    print("Avg time per action log:", actionLogTime / numActionLogs)
if numProfileGets > 0:
    print("Avg time per profile get:", profileGetTime / numProfileGets)
if numProfileSets > 0:
    print("Avg time per profile set:", profileSetTime / numProfileSets)
print("Number of action logs:", numActionLogs)
print("Number of profile gets:", numProfileGets)
print("Number of profile sets:", numProfileSets)
