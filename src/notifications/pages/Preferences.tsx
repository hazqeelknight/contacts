import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const Preferences: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Notification Preferences"
        subtitle="Configure when and how you receive notifications"
      />
      
      <Typography variant="body1">
        Preferences content will be implemented here.
      </Typography>
    </>
  );
};

export default Preferences;