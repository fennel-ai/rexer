/*
Copyright 2017 - 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
    http://aws.amazon.com/apache2.0/
or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/
const axios = require("axios");

var express = require("express");
var bodyParser = require("body-parser");
var cors = require("cors");
var awsServerlessExpressMiddleware = require("aws-serverless-express/middleware");

// declare a new express app
var app = express();
app.use(bodyParser.json());
app.use(awsServerlessExpressMiddleware.eventContext());

// Enable CORS for all methods
var corsOptions = {
  origin: "https://app.fennel.ai",
  optionSuccessStatus: 200, // some legacy browsers (IE11, various SmartTVs) choke on 204
};
app.use(cors(corsOptions));

const domainToURL = {
  "trell.in":
    "http://k8s-ambassad-aesedges-40345becf3-fa1a77f909416990.elb.us-west-2.amazonaws.com/control",
  "fennel.ai":
    "http://k8s-ambassad-aesedges-40345becf3-fa1a77f909416990.elb.us-west-2.amazonaws.com/control",
};

const PROFILE_URL = "profile/";
const ACTION_URL = "actions/";

const actionMetadata = {
  actionTypes: [
    { val: 0, text: "LIKE" },
    { val: 1, text: "SHARE" },
    { val: 2, text: "VIEW" },
  ],
  targetTypes: [
    { val: 0, text: "IMAGE" },
    { val: 1, text: "VIDEO" },
  ],
  actorTypes: [{ val: 0, text: "USER" }],
};

const profileMetadata = {
  oTypes: [
    { val: "USER", text: "USER" },
    { val: "VIDEO", text: "VIDEO" },
  ],
  latestVersion: 1,
};

const mapUserToDomain = (req) => {
  if (!req.query) {
    throw new Error("No query.");
  }
  const username = req.query.username;
  if (!username) {
    throw new Error("Username / email not passed in.");
  }
  const email = username.split("@");
  const tierUrl = domainToURL[email];
  if (tierUrl) {
    return tierUrl;
  } else {
    throw new Error("Domain does not map to a URL.");
  }
};
app.get("/actions/profiles", async (req, res) => {
  try {
    const tierUrl = mapUserToDomain(req);
    const apiUrl = `${tierUrl}/${PROFILE_URL}`;
    const result = await axios.get(apiUrl, {
      params: { key: "hello", otype: "type", oid: 1, version: 1 },
    });
    res.json({ data: req.query });
  } catch (err) {
    res.json({ error: err.message });
  }
});

app.get("/actions/actions", async (req, res) => {
  try {
    const tierUrl = mapUserToDomain(req);
    const apiUrl = `${tierUrl}/${ACTION_URL}`;
    const result = await axios.get(apiUrl, {
      params: { min_action_value: 0 },
    });
    res.json({ data: result.data });
  } catch (err) {
    res.json({ error: err.message });
  }
});

app.get("/actions/actions/metadata", (req, res) => {
  res.json(actionMetadata);
});

app.get("/actions/profiles/metadata", (req, res) => {
  res.json(profileMetadata);
});

app.listen(3001, () => {
  console.log("Server running");
});

// Export the app object. When executing the application local this does nothing. However,
// to port it to AWS Lambda we will create a wrapper around that will load the app from
// this file
module.exports = app;
