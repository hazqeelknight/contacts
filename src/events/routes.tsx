import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Placeholder components - to be implemented by the events module developer
const EventsOverview = React.lazy(() => import('./pages/EventsOverview'));
const EventTypes = React.lazy(() => import('./pages/EventTypes'));
const Bookings = React.lazy(() => import('./pages/Bookings'));
const Analytics = React.lazy(() => import('./pages/Analytics'));

const EventsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route index element={<EventsOverview />} />
      <Route path="types" element={<EventTypes />} />
      <Route path="bookings" element={<Bookings />} />
      <Route path="analytics" element={<Analytics />} />
    </Routes>
  );
};

export default EventsRoutes;