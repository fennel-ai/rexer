import * as React from 'react';
import { API } from 'aws-amplify';
import { ConsoleForm } from './ConsoleForm';
import { ConsoleResult } from './ConsoleResult';
import './../style.css';

const API_ENDPOINT = '/actions';

const getQuery = (form) => {
  const parameters = {};

  if (form.filterActionType.value !== 'ANY') {
    parameters.actionType = form.filterActionType.value;
  }
  if (form.filterTargetId.value !== '') {
    parameters.targetId = form.filterTargetId.value;
  }
  if (form.filterTargetType.value !== 'ANY') {
    parameters.targetType = form.filterTargetType.value;
  }
  if (form.filterActorId.value !== '') {
    parameters.actorId = form.filterActorId.value;
  }
  if (form.filterActorType.value !== 'ANY') {
    parameters.actorType = form.filterActorType.value;
  }
  if (form.filterRequestId.value !== '') {
    parameters.requestId = form.filterRequestId.value;
  }
  if (form.filterStartTime.value !== '') {
    parameters.after = form.filterStartTime.value;
  }
  if (form.filterFinishTime.value !== '') {
    parameters.before = form.filterFinishTime.value;
  }

  return { 'queryStringParameters' : parameters }
};

const Console = () => {
  const [ results, setResults ] = React.useState([]);
  const [ metadata, setMetadata ] = React.useState({notLoaded: true});
  
  React.useEffect(() => {
    API
      .get('bff', `${API_ENDPOINT}/metadata`, {})
      .then(setMetadata)
      .catch((error) => {
        console.log("Failed to load metadata.", error);
      });
  }, []);
  
  const handleQuery = (event) => {
    const form = event.target;
    
    const query = getQuery(form);

    API
      .get('bff', API_ENDPOINT, query)
      .then(response => setResults(response.data))
      .catch(error => console.log(error));

    event.preventDefault();
  }
  
  if( (metadata.notLoaded ?? false) ) {
    return (<div>Loading...</div>);
  } else {
    return (
      <div className="consoleBody">
        <ConsoleForm onQuerySubmit={handleQuery} metadata={metadata} />
        <ConsoleResult results={results} metadata={metadata} />
      </div>
    );
  }
};

export { Console };