import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Placeholder components - to be implemented by the workflows module developer
const WorkflowsOverview = React.lazy(() => import('./pages/WorkflowsOverview'));
const WorkflowsList = React.lazy(() => import('./pages/WorkflowsList'));
const WorkflowBuilder = React.lazy(() => import('./pages/WorkflowBuilder'));
const WorkflowTemplates = React.lazy(() => import('./pages/WorkflowTemplates'));

const WorkflowsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<WorkflowsOverview />} />
      <Route path="list" element={<WorkflowsList />} />
      <Route path="builder" element={<WorkflowBuilder />} />
      <Route path="templates" element={<WorkflowTemplates />} />
    </Routes>
  );
};

export default WorkflowsRoutes;