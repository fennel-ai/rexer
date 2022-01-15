import * as React from 'react';
import { API } from 'aws-amplify';
import { ConsoleSelect } from './ConsoleSelect';
import { ConsoleInput } from './ConsoleInput';
import { ConsoleDateTime } from './ConsoleDateTime';

import './style.css'

const API_ENDPOINT = '/actions/metadata';

const filters = {
  actionType : {
    id: 'filterActionType',
    label: 'Action Type: ',
    options: [
      { val: 'ANY', text: 'ANY' },
    ],
  },
  targetId : {
    id: 'filterTargetId',
    label: 'Target ID: ',
    type: 'number',
  },
  targetType : {
    id: 'filterTargetType',
    label: 'Target Type: ',
    options: [
      { val: 'ANY', text: 'ANY' },
    ],
  },
  actorId : {
    id: 'filterActorId',
    label: 'Actor ID: ',
    type: 'number',
  },
  actorType : {
    id: 'filterActorType',
    label: 'Actor Type: ',
    options: [
      { val: 'ANY', text: 'ANY' },
    ],
  },
  requestId : {
    id: 'filterRequestId',
    label: 'Request ID: ',
    type: 'number',
  },
  startTime: {
    id: 'filterStartTime',
    label: 'After',
  },
  finishTime: {
    id: 'filterFinishTime',
    label: 'Before',
  },
};

const addMetadata = (metadata) => {
  console.log(metadata);
  filters.actionType.options.push(...metadata.actionTypes);
  filters.targetType.options.push(...metadata.targetTypes);
  filters.actorType.options.push(...metadata.actorTypes);
};

const ConsoleForm = ({ onQuerySubmit }) => {
  const [ isReady, setIsReady ] = React.useState(false);
  
  React.useEffect(() => {
    API
      .get('bff', `${API_ENDPOINT}`, {})
      .then(response => {
        addMetadata(response);
        setIsReady(true);
      })
      .catch((error) => {
        console.log("Failed to load metadata: ", error);
      });
  }, []);
    
  return (
    <form onSubmit={onQuerySubmit} className="consoleForm">
      { isReady ? (<>
        <ConsoleSelect data={filters.actionType} />
        <ConsoleInput data={filters.targetId} />
        <ConsoleSelect data={filters.targetType} />
        <ConsoleInput data={filters.actorId} />
        <ConsoleSelect data={filters.actorType} />
        <ConsoleInput data={filters.requestId} />
        <ConsoleDateTime data={filters.startTime} />
        <ConsoleDateTime data={filters.finishTime} />
        
        <div className="consoleFormSubmit">
          <button type="submit" className="consoleFormSubmitButton">Query</button>
        </div>
      </>) : (
        "Loading..."
      )}
    </form>
  );
};

export { ConsoleForm };