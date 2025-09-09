import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Placeholder components - to be implemented by the integrations module developer
const IntegrationsOverview = React.lazy(() => import('./pages/IntegrationsOverview'));
const CalendarIntegrations = React.lazy(() => import('./pages/CalendarIntegrations'));
const VideoIntegrations = React.lazy(() => import('./pages/VideoIntegrations'));
const WebhookIntegrations = React.lazy(() => import('./pages/WebhookIntegrations'));

const IntegrationsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<IntegrationsOverview />} />
      <Route path="calendar" element={<CalendarIntegrations />} />
      <Route path="video" element={<VideoIntegrations />} />
      <Route path="webhooks" element={<WebhookIntegrations />} />
    </Routes>
  );
};

export default IntegrationsRoutes;