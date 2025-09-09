import React from 'react';
import { Routes, Route } from 'react-router-dom';

// Lazy load all user module pages
const UsersOverview = React.lazy(() => import('./pages/UsersOverview'));
const ProfileSettings = React.lazy(() => import('./pages/ProfileSettings'));
const SecuritySettings = React.lazy(() => import('./pages/SecuritySettings'));
const InvitationsList = React.lazy(() => import('./pages/InvitationsList'));
const ActiveSessions = React.lazy(() => import('./pages/ActiveSessions'));
const AuditLogs = React.lazy(() => import('./pages/AuditLogs'));
const MFASettings = React.lazy(() => import('./pages/MFASettings'));
const RolesList = React.lazy(() => import('./pages/RolesList'));
const PermissionsList = React.lazy(() => import('./pages/PermissionsList'));

const UsersRoutes: React.FC = () => {
  return (
    <Routes> 
      {/* Protected users module routes */}
      <Route index element={<UsersOverview />} />
      <Route path="profile" element={<ProfileSettings />} />
      <Route path="security" element={<SecuritySettings />} />
      <Route path="invitations" element={<InvitationsList />} />
      <Route path="sessions" element={<ActiveSessions />} />
      <Route path="audit-logs" element={<AuditLogs />} />
      <Route path="mfa" element={<MFASettings />} />
      <Route path="roles" element={<RolesList />} />
      <Route path="permissions" element={<PermissionsList />} />
    </Routes>
  );
};

export default UsersRoutes;