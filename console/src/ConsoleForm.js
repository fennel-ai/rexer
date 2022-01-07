import * as React from 'react';
import { ConsoleSelect } from './ConsoleSelect';
import { ConsoleInput } from './ConsoleInput';
import { ConsoleDateTime } from './ConsoleDateTime';

import './style.css'

const filters = {
  actionType : {
    id: 'filterActionType',
    label: 'Action Type: ',
    options: [ 'ANY', 'LIKE', 'SHARE' ],
  },
  targetId : {
    id: 'filterTargetId',
    label: 'Target ID: ',
  },
  targetType : {
    id: 'filterTargetType',
    label: 'Target Type: ',
    options: [ 'ANY', 'VIDEO', 'IMAGE' ],
  },
  actorId : {
    id: 'filterActorId',
    label: 'Actor ID: ',
  },
  actorType : {
    id: 'filterActorType',
    label: 'Actor Type: ',
    options: [ 'ANY', 'USER' ],
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

const ConsoleForm = ({ onQuerySubmit }) => {  
  return (
    <form onSubmit={onQuerySubmit} className="consoleForm">
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
    </form>
  );
};

export { ConsoleForm };