import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const WorkflowsList: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Workflows"
        subtitle="View and manage your automated workflows"
      />
      
      <Typography variant="body1">
        Workflows list content will be implemented here.
      </Typography>
    </>
  );
};

export default WorkflowsList;