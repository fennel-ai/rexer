import * as React from "react";
import { API } from "aws-amplify";
import { ConsoleForm } from "./ConsoleForm";
import { ConsoleResult } from "./ConsoleResult";
import { loadLoggedInPage } from "../AuthFunctions";
import { useNavigate } from "react-router-dom";
import "./../style.css";

const API_ENDPOINT = "/actions/actions";

const getQuery = (form) => {
  const params = {};

  if (form.filterActionType.value !== "ANY") {
    params.action_type = form.filterActionType.value;
  }
  if (form.filterTargetId.value !== "") {
    params.target_id = form.filterTargetId.value;
  }
  if (form.filterTargetType.value !== "ANY") {
    params.target_type = form.filterTargetType.value;
  }
  if (form.filterActorId.value !== "") {
    params.actor_id = form.filterActorId.value;
  }
  if (form.filterActorType.value !== "ANY") {
    params.actor_type = form.filterActorType.value;
  }
  if (form.filterRequestId.value !== "") {
    params.request_id = form.filterRequestId.value;
  }
  if (form.filterStartTime.value !== "") {
    params.min_timestamp = form.filterStartTime.value;
  }
  if (form.filterFinishTime.value !== "") {
    params.max_timestamp = form.filterFinishTime.value;
  }
  if (form.filterMinActionId.value !== "") {
    params.min_action_id = form.filterMinActionId.value;
  }
  if (form.filterMaxActionId.value !== "") {
    params.max_action_id = form.filterMaxActionId.value;
  }

  return params
};

const Console = () => {
  const [results, setResults] = React.useState([]);
  const [metadata, setMetadata] = React.useState({ notLoaded: true });
  const [username, setUsername] = React.useState("");
  const navigate = useNavigate();

  React.useEffect(() => {
    loadLoggedInPage(setUsername, navigate);
    API.get("consoleBff", `${API_ENDPOINT}/metadata`, {
      queryStringParameters: {
        username,
      },
    })
      .then(setMetadata)
      .catch((error) => {
        console.log("Failed to load metadata.", error);
      });
  }, []);

  const handleQuery = (event) => {
    const form = event.target;

    const query = getQuery(form);
    query["username"] = username;

    API.get("consoleBff", API_ENDPOINT, {
      queryStringParameters: query,
    })
      .then((response) => setResults(response.data))
      .catch((error) => console.log(error));

    event.preventDefault();
  };

  if (metadata.notLoaded ?? false) {
    return <div>Loading...</div>;
  } else {
    return (
      <div className="consoleBody">
        <ConsoleForm onQuerySubmit={handleQuery} metadata={metadata} />
        <ConsoleResult data={results} metadata={metadata} />
      </div>
    );
  }
};

export { Console };
