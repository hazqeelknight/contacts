export interface Contact {
  id: string;
  first_name: string;
  last_name: string;
  full_name: string;
  email: string;
  phone: string;
  company: string;
  job_title: string;
  notes: string;
  tags: string[];
  total_bookings: number;
  last_booking_date: string | null;
  groups_count: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  groups?: ContactGroup[];
}

export interface ContactGroup {
  id: string;
  name: string;
  description: string;
  color: string;
  contact_count: number;
  contacts: Contact[];
  created_at: string;
  updated_at: string;
}

export interface ContactInteraction {
  id: string;
  contact_name: string;
  contact_id: string;
  interaction_type: string;
  interaction_type_display: string;
  description: string;
  booking_id: string | null;
  metadata: Record<string, any>;
  created_at: string;
}

export interface ContactStats {
  total_contacts: number;
  active_contacts: number;
  total_groups: number;
  recent_interactions: number;
  top_companies: Array<{
    company: string;
    count: number;
  }>;
  booking_frequency: {
    this_month: number;
    last_month: number;
    this_year: number;
  };
}

export interface ContactFilters {
  search?: string;
  group?: string;
  tags?: string;
  is_active?: boolean;
}

export interface TaskStatus {
  task_id: string;
  status: 'PENDING' | 'STARTED' | 'SUCCESS' | 'FAILURE';
  result?: any;
  error?: string;
}