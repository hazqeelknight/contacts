import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const WorkflowsOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Workflows Overview"
        subtitle="Automate your scheduling processes"
      />
      
      <Typography variant="body1">
        Workflows module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default WorkflowsOverview;