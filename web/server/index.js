const express = require('express')
const app = express();

const allLogs = [
  {
    actionType: 'LIKE',
    targetId: '0',
    targetType: 'IMAGE',
    actorId: '0',
    actorType: 'USER',
  },
  {
    actionType: 'LIKE',
    targetId: '1',
    targetType: 'VIDEO',
    actorId: '0',
    actorType: 'USER',
  },
  {
    actionType: 'LIKE',
    targetId: '2',
    targetType: 'IMAGE',
    actorId: '1',
    actorType: 'USER',
  },
  {
    actionType: 'SHARE',
    targetId: '1',
    targetType: 'VIDEO',
    actorId: '1',
    actorType: 'USER',
  },
  {
    actionType: 'SHARE',
    targetId: '2',
    targetType: 'IMAGE',
    actorId: '0',
    actorType: 'USER',
  },
  {
    actionType: 'SHARE',
    targetId: '2',
    targetType: 'VIDEO',
    actorId: '1',
    actorType: 'USER',
  },
    
]

app.get('/api/getlogs', (req, res) => {
  console.log(req.query);
  
  res.json({ result : allLogs.filter((action) => {
    if('actionType' in req.query && req.query.actionType !== action.actionType)
    {
      return false;
    }
    if('targetId' in req.query && req.query.targetId !== action.targetId)
    {
      return false;
    }
    if('targetType' in req.query && req.query.targetType !== action.targetType)
    {
      return false;
    }
    if('actorId' in req.query && req.query.actorId !== action.actorId)
    {
      return false;
    }
    if('actorType' in req.query && req.query.actorType !== action.actorType)
    {
      return false;
    }
    
    return true;
  })});
});

app.listen(3001, () => {
  console.log('Server running');
});
