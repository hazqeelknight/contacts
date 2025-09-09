import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const AvailabilityOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Availability Overview"
        subtitle="Manage when you're available for meetings"
      />
      
      <Typography variant="body1">
        Availability module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default AvailabilityOverview;