import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const EventTypes: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Event Types"
        subtitle="Create and manage your meeting types"
      />
      
      <Typography variant="body1">
        Event types content will be implemented here.
      </Typography>
    </>
  );
};

export default EventTypes;