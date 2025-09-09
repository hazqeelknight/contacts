import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const BlockedTimes: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Blocked Times"
        subtitle="Manage one-time and recurring blocked periods"
      />
      
      <Typography variant="body1">
        Blocked times content will be implemented here.
      </Typography>
    </>
  );
};

export default BlockedTimes;