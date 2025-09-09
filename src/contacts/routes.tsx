import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Placeholder components - to be implemented by the contacts module developer
const ContactsOverview = React.lazy(() => import('./pages/ContactsOverview'));
const ContactsList = React.lazy(() => import('./pages/ContactsList'));
const ContactGroups = React.lazy(() => import('./pages/ContactGroups'));
const ContactInteractions = React.lazy(() => import('./pages/ContactInteractions'));

const ContactsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<ContactsOverview />} />
      <Route path="list" element={<ContactsList />} />
      <Route path="groups" element={<ContactGroups />} />
      <Route path="interactions" element={<ContactInteractions />} />
    </Routes>
  );
};

export default ContactsRoutes;