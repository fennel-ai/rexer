import random
import client
from models import action, counter, value, profile

# --------- CONSTANTS -----------

numActorTypes = 2
numTargetTypes = 2
numActionTypes = 3

numActors = 500
numTargets = 2000
numActions = 1000

outputFname = 'tests/actionLogSmall'

# -------------------------------

def genActors(numActors):
    actors = []
    
    for i in range(numActors):
        actors.append(random.randint(1, numActorTypes))
    
    return actors

def genTargets(numTargets):
    targets = []
    
    for i in range(numTargets):
        targets.append(random.randint(1, numTargetTypes))
    
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
        a.ActionType = random.randint(1, numActionTypes)
        a.RequestID = i+1
        a.CustID = 1
        
        aBytes = list(a.SerializeToString())
        
        f.write(f'{actionLogs[i]} {" ".join(map(str, aBytes))}\n')