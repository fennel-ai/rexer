import random, string
from models import value, profile

# --------- CONSTANTS -----------

GET = 0
SET = 1

oTypes = [ 'User', 'Video' ]
numVersions = 10
maxKeyLen = 10
maxValue = 100

numEvents = 1000

getFname = 'tests/profileGets'
setFname = 'tests/profileSets'

# -------------------------------

def genRandString(maxLen):
    return ''.join(random.choices(
        string.ascii_letters+string.digits,
        k=random.randint(1, maxLen),
    ))

profiles = []

with open(getFname, 'w') as getFile, open(setFname, 'w') as setFile:
    timestamps = random.sample(range(1000000), k=1000)
    timestamps.sort()
    
    for t in timestamps:
        act = random.choice([GET, SET])
        if not profiles:
            act = SET
        
        if act == SET:
            p = profile.ProfileItem()
            p.Oid = len(profiles)+1
            p.OType = random.choice(oTypes)
            p.Key = genRandString(maxKeyLen)
            v = value.Int(random.randint(1, maxValue))
            p.Value.CopyFrom(v)
            p.Version = 1
            
            profiles.append(p)
            
            pBytes = list(p.SerializeToString())
            
            setFile.write(f'{t} {" ".join(map(str, pBytes))}\n')
        else:
            p = random.randint(0, len(profiles)-1)
            pBytes = list(profiles[p].SerializeToString())
            getFile.write(f'{t} {" ".join(map(str, pBytes))}\n')
        