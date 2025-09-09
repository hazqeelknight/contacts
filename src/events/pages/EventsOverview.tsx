import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const EventsOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Events Overview"
        subtitle="Manage your event types and bookings"
      />
      
      <Typography variant="body1">
        Events module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default EventsOverview;