import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Placeholder components - to be implemented by the notifications module developer
const NotificationsOverview = React.lazy(() => import('./pages/NotificationsOverview'));
const Templates = React.lazy(() => import('./pages/Templates'));
const Preferences = React.lazy(() => import('./pages/Preferences'));
const Logs = React.lazy(() => import('./pages/Logs'));

const NotificationsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<NotificationsOverview />} />
      <Route path="templates" element={<Templates />} />
      <Route path="preferences" element={<Preferences />} />
      <Route path="logs" element={<Logs />} />
    </Routes>
  );
};

export default NotificationsRoutes;