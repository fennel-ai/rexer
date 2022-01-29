import * as React from "react";
import axios from "axios";
import { API } from "aws-amplify";
import { ConsoleForm } from "./ConsoleForm";
import { ConsoleResult } from "./ConsoleResult";
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
  if (form.filterMinActionValue.value !== "") {
    params.min_action_value = form.filterMinActionValue.value;
  }
  if (form.filterMaxActionValue.value !== "") {
    params.max_action_value = form.filterMaxActionValue.value;
  }

  return { queryStringParameters: params };
};

const Console = () => {
  const [results, setResults] = React.useState([]);
  const [metadata, setMetadata] = React.useState({ notLoaded: true });

  React.useEffect(() => {
    axios
      .get(`http://localhost:3001${API_ENDPOINT}/metadata`)
      .then((metadata) => setMetadata(metadata.data))
      .catch((error) => {
        console.log("Failed to load metadata.", error);
      });
  }, []);

  const handleQuery = (event) => {
    const form = event.target;

    const query = getQuery(form);

    axios
      .get(`http://localhost:3001${API_ENDPOINT}`, {
        params: {
          query,
        },
      })
      .then((response) => setResults(response.data.data))
      .catch((error) => {
        console.log("Failed to load metadata.", error);
      });

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
