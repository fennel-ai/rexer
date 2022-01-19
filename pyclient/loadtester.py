import time
from operator import itemgetter
import client
from models import action, counter, value, profile

# --------- CONSTANTS -----------

ACTION = 0
PROFILE_GET = 1
PROFILE_SET = 2

actionLogFname = 'tests/actionLogFile'
getProfileFname = 'tests/profileGets'
setProfileFname = 'tests/profileSets'

# ---------- LOADING ------------

def lineParser(l):
    l = l.rstrip().split(' ', 1)
    
    t = int(l[0])
    b = bytes(map(int, l[1].split(' ')))
    
    return (t, b)

c = client.Client()

events = []

with open(actionLogFname) as f:
    (t, b) = lineParser(f.readline())
    
    a = action.Action()
    a.ParseFromString(b)
    
    events.append([t, ACTION, a])

with open(getProfileFname) as f:
    (t, b) = lineParser(f.readline())
    
    p = profile.ProfileItem()
    p.ParseFromString(b)
    
    events.append([t, PROFILE_GET, p])

with open(setProfileFname) as f:
    (t, b) = lineParser(f.readline())
    
    p = profile.ProfileItem()
    p.ParseFromString(b)
    
    events.append([t, PROFILE_SET, p])

events.sort(key=itemgetter(0))

# ----------- MAIN --------------

t = time.time
t0 = t()

i = 0

while i != len(events):
    while t()-t0 < (10**6)*events[i][0]:
        pass
    
    if events[i][1] == ACTION:
        c.log(events[i][2])
    elif events[i][2] == PROFILE_GET:
        c.get_profile(events[i][2])
    else:
        c.set_profile(events[i][2])
    
    i += 1