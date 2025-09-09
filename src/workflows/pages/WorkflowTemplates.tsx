import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const WorkflowTemplates: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Workflow Templates"
        subtitle="Start with pre-built workflow templates"
      />
      
      <Typography variant="body1">
        Workflow templates content will be implemented here.
      </Typography>
    </>
  );
};

export default WorkflowTemplates;