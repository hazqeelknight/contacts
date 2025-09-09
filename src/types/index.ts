// Global type definitions for NovaMeet

export interface User {
  id: string;
  email: string;
  first_name: string;
  last_name: string;
  full_name: string;
  is_organizer: boolean;
  is_email_verified: boolean;
  is_phone_verified: boolean;
  is_mfa_enabled: boolean;
  account_status: 'active' | 'inactive' | 'suspended' | 'pending_verification' | 'password_expired' | 'password_expired_grace_period';
  roles: Role[];
  profile: Profile;
  last_login: string | null;
  date_joined: string;
}

export interface Profile {
  organizer_slug: string;
  display_name: string;
  bio: string;
  profile_picture: string | null;
  phone: string;
  website: string;
  company: string;
  job_title: string;
  timezone_name: string;
  language: string;
  date_format: string;
  time_format: string;
  brand_color: string;
  brand_logo: string | null;
  public_profile: boolean;
  show_phone: boolean;
  show_email: boolean;
  reasonable_hours_start: number;
  reasonable_hours_end: number;
}

export interface Role {
  id: string;
  name: string;
  role_type: 'admin' | 'organizer' | 'team_member' | 'billing_manager' | 'viewer';
  description: string;
  parent: string | null;
  parent_name: string | null;
  children_count: number;
  role_permissions: Permission[];
  total_permissions: number;
  is_system_role: boolean;
}

export interface Permission {
  id: string;
  codename: string;
  name: string;
  description: string;
  category: string;
}

export interface EventType {
  id: string;
  organizer: User;
  name: string;
  event_type_slug: string;
  description: string;
  duration: number;
  max_attendees: number;
  enable_waitlist: boolean;
  is_active: boolean;
  is_private: boolean;
  min_scheduling_notice: number;
  max_scheduling_horizon: number;
  buffer_time_before: number;
  buffer_time_after: number;
  max_bookings_per_day: number | null;
  slot_interval_minutes: number;
  recurrence_type: 'none' | 'daily' | 'weekly' | 'monthly';
  recurrence_rule: string;
  max_occurrences: number | null;
  recurrence_end_date: string | null;
  location_type: 'video_call' | 'phone_call' | 'in_person' | 'custom';
  location_details: string;
  redirect_url_after_booking: string;
  questions: CustomQuestion[];
  is_group_event: boolean;
  total_duration_with_buffers: number;
  created_at: string;
  updated_at: string;
}

export interface CustomQuestion {
  id: string;
  question_text: string;
  question_type: 'text' | 'textarea' | 'select' | 'multiselect' | 'checkbox' | 'radio' | 'email' | 'phone' | 'number' | 'date' | 'time' | 'url';
  question_type_display: string;
  is_required: boolean;
  order: number;
  options: string[];
  conditions: any[];
  validation_rules: Record<string, any>;
}

export interface Booking {
  id: string;
  event_type: EventType;
  organizer: User;
  invitee_name: string;
  invitee_email: string;
  invitee_phone: string;
  invitee_timezone: string;
  attendee_count: number;
  start_time: string;
  end_time: string;
  status: 'confirmed' | 'cancelled' | 'rescheduled' | 'completed' | 'no_show';
  status_display: string;
  recurrence_id: string | null;
  is_recurring_exception: boolean;
  recurrence_sequence: number | null;
  custom_answers: Record<string, any>;
  meeting_link: string;
  meeting_id: string;
  meeting_password: string;
  calendar_sync_status: 'pending' | 'succeeded' | 'failed' | 'not_required';
  attendees: Attendee[];
  duration_minutes: number;
  can_cancel: boolean;
  can_reschedule: boolean;
  is_access_token_valid: boolean;
  cancelled_at: string | null;
  cancelled_by: 'organizer' | 'invitee' | 'system' | null;
  cancellation_reason: string;
  rescheduled_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface Attendee {
  id: string;
  name: string;
  email: string;
  phone: string;
  status: 'confirmed' | 'cancelled' | 'no_show';
  status_display: string;
  custom_answers: Record<string, any>;
  joined_at: string;
  cancelled_at: string | null;
  cancellation_reason: string;
}

export interface AvailableSlot {
  start_time: string;
  end_time: string;
  duration_minutes: number;
  local_start_time?: string;
  local_end_time?: string;
  invitee_times?: Record<string, any>;
  fairness_score?: number;
}

// API Response types
export interface ApiResponse<T = any> {
  data?: T;
  message?: string;
  error?: string;
  errors?: Record<string, string[]>;
}

export interface PaginatedResponse<T> {
  count: number;
  next: string | null;
  previous: string | null;
  results: T[];
}

// Common UI types
export interface SelectOption {
  value: string | number;
  label: string;
  disabled?: boolean;
}

export interface TableColumn<T = any> {
  id: keyof T;
  label: string;
  minWidth?: number;
  align?: 'left' | 'center' | 'right';
  format?: (value: any) => string;
  sortable?: boolean;
}

// Form types
export interface FormField {
  name: string;
  label: string;
  type: 'text' | 'email' | 'password' | 'number' | 'select' | 'multiselect' | 'textarea' | 'checkbox' | 'radio' | 'date' | 'time';
  required?: boolean;
  options?: SelectOption[];
  validation?: Record<string, any>;
  placeholder?: string;
  helperText?: string;
}

// Animation types for Framer Motion
export interface AnimationVariants {
  initial: Record<string, any>;
  animate: Record<string, any>;
  exit?: Record<string, any>;
  transition?: Record<string, any>;
}

// Theme extensions
export interface CustomTheme {
  customShadows: {
    card: string;
    dropdown: string;
    modal: string;
    tooltip: string;
  };
}

// Navigation types
export interface NavigationItem {
  id: string;
  label: string;
  path: string;
  icon?: React.ComponentType;
  children?: NavigationItem[];
  permission?: string;
  badge?: string | number;
}

// Notification types
export interface NotificationItem {
  id: string;
  type: 'success' | 'error' | 'warning' | 'info';
  title: string;
  message: string;
  timestamp: string;
  read: boolean;
  action?: {
    label: string;
    onClick: () => void;
  };
}