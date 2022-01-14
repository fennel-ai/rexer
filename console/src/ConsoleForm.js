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
    options: [ 'ANY' ],
  },
  targetId : {
    id: 'filterTargetId',
    label: 'Target ID: ',
  },
  targetType : {
    id: 'filterTargetType',
    label: 'Target Type: ',
    options: [ 'ANY' ],
  },
  actorId : {
    id: 'filterActorId',
    label: 'Actor ID: ',
  },
  actorType : {
    id: 'filterActorType',
    label: 'Actor Type: ',
    options: [ 'ANY' ],
  },
  requestId : {
    id: 'filterRequestId',
    label: 'Request ID: ',
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
  filters.actionType.options.push(...metadata.actionType);
  filters.targetType.options.push(...metadata.targetType);
  filters.actorType.options.push(...metadata.actorType);
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
        console.log("Failed to load metadata");
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