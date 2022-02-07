import random, sys
from models import action

# --------- CONSTANTS -----------

actorTypes = [ 'USER' ]
targetTypes = [ 'IMAGE', 'VIDEO']
actionTypes = [ 'LIKE', 'SHARE', 'VIEW' ]

numActors = 500
numTargets = 2000
numActions = 10000
if len(sys.argv) >= 2:
    numActions = int(sys.argv[1])

outputFname = 'tests/actionLog'
if len(sys.argv) >= 3 and sys.argv[2] == '-s':
    outputFname += 'Small'

# -------------------------------

def genActors(numActors):
    actors = []
    
    for i in range(numActors):
        actors.append(random.choice(actorTypes))
    
    return actors

def genTargets(numTargets):
    targets = []
    
    for i in range(numTargets):
        targets.append(random.choice(targetTypes))
    
    return targets

actionLogs = random.sample(range(1000000), k=numActions)
actionLogs.sort()

actors = genActors(numActors)
targets = genTargets(numTargets)

with open(outputFname, 'w') as f:
    for i in range(len(actionLogs)):
        actor = random.randint(1, numActors)
        target = random.randint(1, numTargets)
        
        a = action.Action()
        a.ActorID = actor
        a.ActorType = actors[actor-1]
        a.TargetID = target
        a.TargetType = targets[target-1]
        a.ActionType = random.choice(actionTypes)
        a.RequestID = i+1
        
        aBytes = list(a.SerializeToString())
        
        f.write(f'{actionLogs[i]} {" ".join(map(str, aBytes))}\n')