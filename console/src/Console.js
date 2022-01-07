import * as React from 'react';
import { API } from 'aws-amplify';
import { ConsoleForm } from './ConsoleForm';
import { ConsoleResult } from './ConsoleResult';
import './style.css'

const API_ENDPOINT = '/actions?';

const getQueryUrl = (form) => {
  let queryUrl = API_ENDPOINT;

  if (form.filterActionType.value !== 'ANY') {
    queryUrl += `actionType=${form.filterActionType.value}&`;
  }
  if (form.filterTargetId.value !== '') {
    queryUrl += `targetId=${form.filterTargetId.value}&`;
  }
  if (form.filterTargetType.value !== 'ANY') {
    queryUrl += `targetType=${form.filterTargetType.value}&`;
  }
  if (form.filterActorId.value !== '') {
    queryUrl += `actorId=${form.filterActorId.value}&`;
  }
  if (form.filterActorType.value !== 'ANY') {
    queryUrl += `actorType=${form.filterActorType.value}&`;
  }
  if (form.filterRequestId.value !== '') {
    queryUrl += `requestId=${form.filterRequestId.value}&`;
  }
  if (form.filterStartTime.value !== '') {
    queryUrl += `after=${form.filterStartTime.value}&`;
  }
  if (form.filterFinishTime.value !== '') {
    queryUrl += `before=${form.filterFinishTime.value}&`;
  }

  queryUrl = queryUrl.slice(0, -1);

  console.log(queryUrl);

  return queryUrl;
}

const Console = () => {
  const updateData = React.useRef();

  const handleQuery = (event) => {
    const form = event.target;

    let queryUrl = getQueryUrl(form);

    API
      .get('bff', queryUrl, {})
      .then(response => {
        console.log(response);
        updateData.current(response.data);
      })
      .catch(error => {
        console.log(error);
      });

    event.preventDefault();
  }

  return (
    <div className="consoleBody">
      <ConsoleForm onQuerySubmit={handleQuery} />
      <ConsoleResult updateData={updateData} />
    </div>
  );
};

export { Console };