import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const IntegrationsOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Integrations Overview"
        subtitle="Connect NovaMeet with your favorite tools"
      />
      
      <Typography variant="body1">
        Integrations module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default IntegrationsOverview;