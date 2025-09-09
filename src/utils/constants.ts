// API endpoints
export const API_ENDPOINTS = {
  // Users
  USERS: {
    REGISTER: '/users/register/',
    LOGIN: '/users/login/',
    LOGOUT: '/users/logout/',
    PROFILE: '/users/profile/',
    CHANGE_PASSWORD: '/users/change-password/',
    REQUEST_PASSWORD_RESET: '/users/request-password-reset/',
    CONFIRM_PASSWORD_RESET: '/users/confirm-password-reset/',
    VERIFY_EMAIL: '/users/verify-email/',
    RESEND_VERIFICATION: '/users/resend-verification/',
    SESSIONS: '/users/sessions/',
    AUDIT_LOGS: '/users/audit-logs/',
    ROLES: '/users/roles/',
    PERMISSIONS: '/users/permissions/',
    INVITATIONS: '/users/invitations/',
    REQUEST_PHONE_VERIFICATION: '/users/request-phone-verification/',
    VERIFY_PHONE_NUMBER: '/users/verify-phone-number/',
    // Admin endpoints
    ADMIN_ROLES: '/users/admin/roles/',
    ADMIN_PERMISSIONS: '/users/admin/permissions/',
  },
  
  // Events
  EVENTS: {
    EVENT_TYPES: '/events/event-types/',
    BOOKINGS: '/events/bookings/',
    CREATE_BOOKING: '/events/bookings/create/',
    PUBLIC_ORGANIZER: (slug: string) => `/events/public/${slug}/`,
    PUBLIC_EVENT_TYPE: (organizerSlug: string, eventSlug: string) => 
      `/events/public/${organizerSlug}/${eventSlug}/`,
    AVAILABLE_SLOTS: (organizerSlug: string, eventSlug: string) => 
      `/events/slots/${organizerSlug}/${eventSlug}/`,
    ANALYTICS: '/events/analytics/',
  },
  
  // Availability
  AVAILABILITY: {
    RULES: '/availability/rules/',
    OVERRIDES: '/availability/overrides/',
    BLOCKED_TIMES: '/availability/blocked/',
    RECURRING_BLOCKS: '/availability/recurring-blocks/',
    BUFFER_SETTINGS: '/availability/buffer/',
    CALCULATED_SLOTS: (organizerSlug: string) => `/availability/calculated-slots/${organizerSlug}/`,
    STATS: '/availability/stats/',
  },
  
  // Integrations
  INTEGRATIONS: {
    CALENDAR: '/integrations/calendar/',
    VIDEO: '/integrations/video/',
    WEBHOOKS: '/integrations/webhooks/',
    LOGS: '/integrations/logs/',
    OAUTH_INITIATE: '/integrations/oauth/initiate/',
    OAUTH_CALLBACK: '/integrations/oauth/callback/',
    HEALTH: '/integrations/health/',
  },
  
  // Notifications
  NOTIFICATIONS: {
    TEMPLATES: '/notifications/templates/',
    LOGS: '/notifications/logs/',
    PREFERENCES: '/notifications/preferences/',
    SCHEDULED: '/notifications/scheduled/',
    SEND: '/notifications/send/',
    STATS: '/notifications/stats/',
    HEALTH: '/notifications/health/',
  },
  
  // Contacts
  CONTACTS: {
    LIST: '/contacts/',
    GROUPS: '/contacts/groups/',
    INTERACTIONS: '/contacts/interactions/',
    STATS: '/contacts/stats/',
    IMPORT: '/contacts/import/',
    EXPORT: '/contacts/export/',
    MERGE: '/contacts/merge/',
  },
  
  // Workflows
  WORKFLOWS: {
    LIST: '/workflows/',
    ACTIONS: (workflowId: string) => `/workflows/${workflowId}/actions/`,
    EXECUTIONS: '/workflows/executions/',
    TEMPLATES: '/workflows/templates/',
    PERFORMANCE_STATS: '/workflows/performance-stats/',
  },
} as const;

// Status constants
export const BOOKING_STATUSES = {
  CONFIRMED: 'confirmed',
  CANCELLED: 'cancelled',
  RESCHEDULED: 'rescheduled',
  COMPLETED: 'completed',
  NO_SHOW: 'no_show',
} as const;

export const ACCOUNT_STATUSES = {
  ACTIVE: 'active',
  INACTIVE: 'inactive',
  SUSPENDED: 'suspended',
  PENDING_VERIFICATION: 'pending_verification',
  PASSWORD_EXPIRED: 'password_expired',
  PASSWORD_EXPIRED_GRACE_PERIOD: 'password_expired_grace_period',
} as const;

export const EVENT_LOCATION_TYPES = {
  VIDEO_CALL: 'video_call',
  PHONE_CALL: 'phone_call',
  IN_PERSON: 'in_person',
  CUSTOM: 'custom',
} as const;

// UI constants
export const DRAWER_WIDTH = 280;
export const DRAWER_WIDTH_COLLAPSED = 72;

// Animation constants
export const ANIMATION_DURATION = {
  FAST: 0.2,
  NORMAL: 0.3,
  SLOW: 0.5,
} as const;

// Form validation constants
export const VALIDATION_RULES = {
  PASSWORD_MIN_LENGTH: 8,
  NAME_MAX_LENGTH: 100,
  EMAIL_MAX_LENGTH: 254,
  PHONE_MAX_LENGTH: 20,
  DESCRIPTION_MAX_LENGTH: 1000,
} as const;

// Date/time constants
export const DATE_FORMATS = {
  DISPLAY: 'MMM dd, yyyy',
  DISPLAY_WITH_TIME: 'MMM dd, yyyy h:mm a',
  ISO: 'yyyy-MM-dd',
  TIME: 'h:mm a',
} as const;

// Pagination constants
export const PAGINATION = {
  DEFAULT_PAGE_SIZE: 20,
  PAGE_SIZE_OPTIONS: [10, 20, 50, 100],
} as const;

// File upload constants
export const FILE_UPLOAD = {
  MAX_SIZE_MB: 10,
  ALLOWED_IMAGE_TYPES: ['image/jpeg', 'image/png', 'image/gif', 'image/webp'],
  ALLOWED_DOCUMENT_TYPES: ['application/pdf', 'text/csv', 'application/vnd.ms-excel'],
  PROFILE_IMAGE_MAX_SIZE_MB: 5,
  BRAND_LOGO_MAX_SIZE_MB: 2,
} as const;