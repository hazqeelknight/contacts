import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const Logs: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Notification Logs"
        subtitle="View the history of sent notifications"
      />
      
      <Typography variant="body1">
        Logs content will be implemented here.
      </Typography>
    </>
  );
};

export default Logs;