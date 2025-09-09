import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Lazy load all contact pages
const ContactsOverview = React.lazy(() => import('./pages/ContactsOverview'));
const ContactsList = React.lazy(() => import('./pages/ContactsList'));
const ContactDetail = React.lazy(() => import('./pages/ContactDetail'));
const ContactGroups = React.lazy(() => import('./pages/ContactGroups'));
const ContactGroupDetail = React.lazy(() => import('./pages/ContactGroupDetail'));
const ContactInteractions = React.lazy(() => import('./pages/ContactInteractions'));

const ContactsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<ContactsOverview />} />
      <Route path="list" element={<ContactsList />} />
      <Route path="detail/:id" element={<ContactDetail />} />
      <Route path="groups" element={<ContactGroups />} />
      <Route path="groups/:id" element={<ContactGroupDetail />} />
      <Route path="interactions" element={<ContactInteractions />} />
    </Routes>
  );
};

export default ContactsRoutes;