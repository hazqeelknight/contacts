import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const Templates: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Notification Templates"
        subtitle="Create and manage email and SMS templates"
      />
      
      <Typography variant="body1">
        Templates content will be implemented here.
      </Typography>
    </>
  );
};

export default Templates;