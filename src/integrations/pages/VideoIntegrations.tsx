import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const VideoIntegrations: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Video Integrations"
        subtitle="Set up video conferencing for your meetings"
      />
      
      <Typography variant="body1">
        Video integrations content will be implemented here.
      </Typography>
    </>
  );
};

export default VideoIntegrations;