import * as React from "react";
import { API } from "aws-amplify";
import { loadLoggedInPage } from "../AuthFunctions";
import { ConsoleForm } from "./ConsoleForm";
import { ConsoleResult } from "./ConsoleResult";
import { useNavigate } from "react-router-dom";
import "./../style.css";

const API_ENDPOINT = "/actions/profiles";

const getQuery = (form) => {
  const params = {};

  if (form.filterOType.value !== "ANY") {
    params.otype = form.filterOType.value;
  }
  if (form.filterOId.value !== "") {
    params.oid = form.filterOId.value;
  }
  if (form.filterKey.value !== "") {
    params.key = form.filterKey.value;
  }
  if (form.filterVersion.value !== "") {
    params.version = form.filterVersion.value;
  }

  return params;
};

const Console = () => {
  const [results, setResults] = React.useState([]);
  const [username, setUsername] = React.useState("");
  const navigate = useNavigate();

  React.useEffect(() => {
    loadLoggedInPage(setUsername, navigate);
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

  return (
    <div className="consoleBody">
      <ConsoleForm onQuerySubmit={handleQuery} />
      <ConsoleResult results={results} />
    </div>
  );
};

export { Console };
