import * as React from 'react';
import { ConsoleSelect } from './../ConsoleSelect';
import { ConsoleInput } from './../ConsoleInput';
import { ConsoleDateTime } from './../ConsoleDateTime';
import './../style.css';

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
  minActionId: {
    id: 'filterMinActionId',
    label: 'Min Action ID: ',
    type: 'number',
  },
  maxActionId: {
    id: 'filterMaxActionId',
    label: 'Max Action ID: ',
    type: 'number',
  },
};

const ConsoleForm = ({ onQuerySubmit, metadata }) => {
  return (
    <form onSubmit={onQuerySubmit} className="consoleForm">
      <ConsoleSelect data={filters.actionType} more={metadata.actionTypes} />
      <ConsoleInput data={filters.targetId} />
      <ConsoleSelect data={filters.targetType} more={metadata.targetTypes} />
      <ConsoleInput data={filters.actorId} />
      <ConsoleSelect data={filters.actorType} more={metadata.actorTypes} />
      <ConsoleInput data={filters.requestId} />
      <ConsoleDateTime data={filters.startTime} />
      <ConsoleDateTime data={filters.finishTime} />
      <ConsoleInput data={filters.minActionId} />
      <ConsoleInput data={filters.maxActionId} />
      
      <div className="consoleFormSubmit">
        <button type="submit" className="consoleFormSubmitButton">Query</button>
      </div>
    </form>
  );
};

export { ConsoleForm };