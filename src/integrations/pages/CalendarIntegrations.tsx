import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const CalendarIntegrations: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Calendar Integrations"
        subtitle="Connect your calendars for seamless scheduling"
      />
      
      <Typography variant="body1">
        Calendar integrations content will be implemented here.
      </Typography>
    </>
  );
};

export default CalendarIntegrations;