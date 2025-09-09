import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const NotificationsOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Notifications Overview"
        subtitle="Manage your notification settings and templates"
      />
      
      <Typography variant="body1">
        Notifications module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default NotificationsOverview;