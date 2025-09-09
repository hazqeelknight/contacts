import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const Bookings: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Bookings"
        subtitle="View and manage all your scheduled meetings"
      />
      
      <Typography variant="body1">
        Bookings content will be implemented here.
      </Typography>
    </>
  );
};

export default Bookings;