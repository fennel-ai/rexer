import * as React from 'react';

import { ConsoleSelect } from './ConsoleSelect';
import { ConsoleInput } from './ConsoleInput';

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
};

const ConsoleForm = ({ onQuerySubmit }) => {  
  return (
    <form onSubmit={onQuerySubmit}>
      <ConsoleSelect data={filters.actionType} />
      <ConsoleInput data={filters.targetId} />
      <ConsoleSelect data={filters.targetType} />
      <ConsoleInput data={filters.actorId} />
      <ConsoleSelect data={filters.actorType} />
      <ConsoleInput data={filters.requestId} />
      
      <button type="submit">Query</button>
    </form>
  );
};

export { ConsoleForm };