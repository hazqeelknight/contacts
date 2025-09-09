import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const WebhookIntegrations: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Webhook Integrations"
        subtitle="Configure webhooks for external system integration"
      />
      
      <Typography variant="body1">
        Webhook integrations content will be implemented here.
      </Typography>
    </>
  );
};

export default WebhookIntegrations;