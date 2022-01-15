/*
Copyright 2017 - 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
    http://aws.amazon.com/apache2.0/
or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/




var express = require('express')
var bodyParser = require('body-parser')
var awsServerlessExpressMiddleware = require('aws-serverless-express/middleware')

// declare a new express app
var app = express()
app.use(bodyParser.json())
app.use(awsServerlessExpressMiddleware.eventContext())

// Enable CORS for all methods
app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*")
  res.header("Access-Control-Allow-Headers", "*")
  next()
});

const allActions = [
  {
    actionId: 0,
    actionType: 'LIKE',
    targetId: '0',
    targetType: 'IMAGE',
    actorId: '0',
    actorType: 'USER',
    requestId: '0',
    timestamp: Date.parse('2022-01-01'),
  },
  {
    actionId: 1,
    actionType: 'LIKE',
    targetId: '1',
    targetType: 'VIDEO',
    actorId: '0',
    actorType: 'USER',
    requestId: '0',
    timestamp: Date.parse('2022-01-03'),
  },
  {
    actionId: 2,
    actionType: 'LIKE',
    targetId: '2',
    targetType: 'IMAGE',
    actorId: '1',
    actorType: 'USER',
    requestId: '2',
    timestamp: Date.parse('2022-01-05'),
  },
  {
    actionId: 3,
    actionType: 'SHARE',
    targetId: '1',
    targetType: 'VIDEO',
    actorId: '1',
    actorType: 'USER',
    requestId: '2',
    timestamp: Date.parse('2022-01-04'),
  },
  {
    actionId: 4,
    actionType: 'SHARE',
    targetId: '2',
    targetType: 'IMAGE',
    actorId: '0',
    requestId: '0',
    actorType: 'USER',
    timestamp: Date.parse('2022-01-02'),
  },
  {
    actionId: 5,
    actionType: 'SHARE',
    targetId: '2',
    targetType: 'VIDEO',
    actorId: '1',
    actorType: 'USER',
    requestId: '3',
    timestamp: Date.parse('2022-01-07'),
  },
];

const actionMetadata = {
  actionTypes: [ 'LIKE', 'SHARE' ],
  targetTypes: [ 'IMAGE', 'VIDEO' ],
  actorTypes: [ 'USER' ],
};

app.get('/actions', (req, res) => {
  res.json({
    data: allActions.filter((action) => {
      if ('actionType' in req.query && req.query.actionType !== action.actionType) {
        return false;
      }
      if ('targetId' in req.query && req.query.targetId !== action.targetId) {
        return false;
      }
      if ('targetType' in req.query && req.query.targetType !== action.targetType) {
        return false;
      }
      if ('actorId' in req.query && req.query.actorId !== action.actorId) {
        return false;
      }
      if ('actorType' in req.query && req.query.actorType !== action.actorType) {
        return false;
      }
      if ('requestId' in req.query && req.query.requestId !== action.requestId) {
        return false;
      }
      if ('before' in req.query && Date.parse(req.query.before) < action.timestamp) {
        return false;
      }
      if ('after' in req.query && Date.parse(req.query.after) > action.timestamp) {
        return false;
      }

      return true;
    })
  });
});

app.get('/actions/metadata', (req, res) => {
  res.json(actionMetadata)
});

app.listen(3001, () => {
  console.log('Server running');
});

// Export the app object. When executing the application local this does nothing. However,
// to port it to AWS Lambda we will create a wrapper around that will load the app from
// this file
module.exports = app
