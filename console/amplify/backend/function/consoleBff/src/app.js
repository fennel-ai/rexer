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
    actionType: 0,
    targetId: 0,
    targetType: 0,
    actorId: 0,
    actorType: 0,
    requestId: 0,
    actionValue: 0,
    timestamp: Date.parse('2022-01-01'),
  },
  {
    actionId: 1,
    actionType: 0,
    targetId: 1,
    targetType: 1,
    actorId: 0,
    actorType: 0,
    requestId: 0,
    actionValue: 1,
    timestamp: Date.parse('2022-01-03'),
  },
  {
    actionId: 2,
    actionType: 0,
    targetId: 2,
    targetType: 0,
    actorId: 1,
    actorType: 0,
    requestId: 2,
    actionValue: 4,
    timestamp: Date.parse('2022-01-05'),
  },
  {
    actionId: 3,
    actionType: 1,
    targetId: 1,
    targetType: 1,
    actorId: 1,
    actorType: 0,
    requestId: 2,
    actionValue: 9,
    timestamp: Date.parse('2022-01-04'),
  },
  {
    actionId: 4,
    actionType: 1,
    targetId: 2,
    targetType: 0,
    actorId: 0,
    actorType: 0,
    requestId: 0,
    actionValue: 16,
    timestamp: Date.parse('2022-01-02'),
  },
  {
    actionId: 5,
    actionType: 1,
    targetId: 2,
    targetType: 1,
    actorId: 1,
    actorType: 0,
    requestId: 3,
    actionValue: 25,
    timestamp: Date.parse('2022-01-07'),
  },
];

const allProfiles = [
  {
    oId: 0,
    oType: 0,
    key: 'str0',
    version: 0,
  },
  {
    oId: 1,
    oType: 1,
    key: 'str1',
    version: 0,
  },
  {
    oId: 2,
    oType: 0,
    key: 'str2',
    version: 1,
  },
  {
    oId: 3,
    oType: 1,
    key: 'str3',
    version: 1,
  },
];

const actionMetadata = {
  actionTypes: [ 
    { val:0, text:'LIKE' },
    { val:1, text:'SHARE' },
    { val:2, text:'VIEW' },
  ],
  targetTypes: [
    { val:0, text:'IMAGE' },
    { val:1, text:'VIDEO' },
  ],
  actorTypes: [
    { val:0, text:'USER' },
  ],
};

const profileMetadata = {
  latestVersion: 1,
};

app.get('/actions/actions', (req, res) => {
  res.json({
    data: allActions.filter((action) => {
      if ('action_type' in req.query && Number(req.query.action_type) !== action.actionType) {
        return false;
      }
      if ('target_id' in req.query && Number(req.query.target_id) !== action.targetId) {
        return false;
      }
      if ('target_type' in req.query && Number(req.query.target_type) !== action.targetType) {
        return false;
      }
      if ('actor_id' in req.query && Number(req.query.actor_id) !== action.actorId) {
        return false;
      }
      if ('actor_type' in req.query && Number(req.query.actor_type) !== action.actorType) {
        return false;
      }
      if ('request_id' in req.query && Number(req.query.request_id) !== action.requestId) {
        return false;
      }
      if ('max_timestamp' in req.query && Date.parse(req.query.max_timestamp) < action.timestamp) {
        return false;
      }
      if ('min_timestamp' in req.query && Date.parse(req.query.min_timestamp) > action.timestamp) {
        return false;
      }
      if ('min_action_id' in req.query && req.query.min_action_id > action.actionId) {
        return false;
      }
      if ('max_action_id' in req.query && req.query.max_action_id < action.actionId) {
        return false;
      }
      if ('min_action_value' in req.query && req.query.min_action_value > action.actionValue) {
        return false;
      }
      if ('max_action_value' in req.query && req.query.max_action_value < action.actionValue) {
        return false;
      }

      return true;
    }),
  });
});

app.get('/actions/profiles', (req, res) => {
  res.json({
    data: allProfiles.filter((profile) => {
      if ('oid' in req.query && Number(req.query.oid) !== profile.oId) {
        return false;
      }
      if ('otype' in req.query && Number(req.query.otype) !== profile.oType) {
        return false;
      }
      if ('key' in req.query && req.query.key !== profile.key) {
        return false;
      }
      if ('version' in req.query && req.query.version !== profile.version) {
        return false;
      }
      
      return true;
    }),
  });
});

app.get('/actions/actions/metadata', (req, res) => {
  res.json(actionMetadata);
});

app.get('/actions/profiles/metadata', (req, res) => {
  res.json(profileMetadata);
});

app.listen(3001, () => {
  console.log('Server running');
});

// Export the app object. When executing the application local this does nothing. However,
// to port it to AWS Lambda we will create a wrapper around that will load the app from
// this file
module.exports = app
  