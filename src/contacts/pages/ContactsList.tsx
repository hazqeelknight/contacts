import React from 'react';
import { Typography } from '@mui/material';
import { PageHeader } from '@/components/core';

const ContactsList: React.FC = () => {
  return (
    <>
      <PageHeader
        title="Contacts List"
        subtitle="View and manage all your contacts"
      />
      
      <Typography variant="body1">
        Contacts list content will be implemented here.
      </Typography>
    </>
  );
};

export default ContactsList;