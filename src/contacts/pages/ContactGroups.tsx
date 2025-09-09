import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const ContactGroups: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Contact Groups"
        subtitle="Organize your contacts into groups"
      />
      
      <Typography variant="body1">
        Contact groups content will be implemented here.
      </Typography>
    </>
  );
};

export default ContactGroups;