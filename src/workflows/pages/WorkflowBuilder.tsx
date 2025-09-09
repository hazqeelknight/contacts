import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const WorkflowBuilder: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Workflow Builder"
        subtitle="Create and edit automated workflows"
      />
      
      <Typography variant="body1">
        Workflow builder content will be implemented here.
      </Typography>
    </>
  );
};

export default WorkflowBuilder;