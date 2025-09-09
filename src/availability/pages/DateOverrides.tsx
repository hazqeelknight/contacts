import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const DateOverrides: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Date Overrides"
        subtitle="Override your schedule for specific dates"
      />
      
      <Typography variant="body1">
        Date overrides content will be implemented here.
      </Typography>
    </>
  );
};

export default DateOverrides;