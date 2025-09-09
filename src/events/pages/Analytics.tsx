import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const Analytics: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Analytics"
        subtitle="Insights into your booking performance"
      />
      
      <Typography variant="body1">
        Analytics content will be implemented here.
      </Typography>
    </>
  );
};

export default Analytics;