import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const ContactsOverview: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Contacts Overview"
        subtitle="Manage your contact database and interactions"
      />
      
      <Typography variant="body1">
        Contacts module content will be implemented here by the assigned developer.
      </Typography>
    </>
  );
};

export default ContactsOverview;