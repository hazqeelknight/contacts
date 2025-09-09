import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const AvailabilityRules: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Availability Rules"
        subtitle="Set your recurring weekly schedule"
      />
      
      <Typography variant="body1">
        Availability rules content will be implemented here.
      </Typography>
    </>
  );
};

export default AvailabilityRules;