import React from 'react';
import {
  Box,
  TextField,
  MenuItem,
  Chip,
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Paper,
  Checkbox,
  Avatar,
} from '@mui/material';
import {
  Add,
  Edit,
  Delete,
  Email,
  Phone,
  Business,
  FileUpload,
  FileDownload,
  MergeType,
  Visibility,
} from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { PageHeader } from '@/components/core';
import { Button, LoadingSpinner } from '@/components/core';
import {
  useContacts,
  useDeleteContact,
  useExportContacts,
  useMergeContacts,
  useContactGroups,
  useCreateContact,
} from '../api';
import { ContactForm } from '../components/ContactForm';
import { ContactImportModal } from '../components/ContactImportModal';
import { ContactMergeModal } from '../components/ContactMergeModal';
import { Contact, ContactFilters } from '../types';
import { formatDate } from '@/utils/formatters';

const ContactsList: React.FC = () => {
  const navigate = useNavigate();
  const [filters, setFilters] = React.useState<ContactFilters>({});
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(20);
  const [selectedContacts, setSelectedContacts] = React.useState<string[]>([]);
  const [contactFormOpen, setContactFormOpen] = React.useState(false);
  const [importModalOpen, setImportModalOpen] = React.useState(false);
  const [mergeModalOpen, setMergeModalOpen] = React.useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = React.useState(false);
  const [contactToDelete, setContactToDelete] = React.useState<Contact | null>(null);

  const { data: contactsData, isLoading } = useContacts(filters);
  const { data: groupsData } = useContactGroups();
  const deleteContactMutation = useDeleteContact();
  const exportContactsMutation = useExportContacts();
  const mergeContactsMutation = useMergeContacts();
  const createContactMutation = useCreateContact();

  const contacts = contactsData?.results || [];
  const groups = groupsData?.results || [];
  const totalContacts = contactsData?.count || 0;

  const handleFilterChange = (key: keyof ContactFilters, value: any) => {
    setFilters(prev => ({ ...prev, [key]: value }));
    setPage(0);
  };

  const handleSelectContact = (contactId: string) => {
    setSelectedContacts(prev =>
      prev.includes(contactId)
        ? prev.filter(id => id !== contactId)
        : [...prev, contactId]
    );
  };

  const handleSelectAll = () => {
    if (selectedContacts.length === contacts.length) {
      setSelectedContacts([]);
    } else {
      setSelectedContacts(contacts.map(c => c.id));
    }
  };

  const handleDeleteContact = (contact: Contact) => {
    setContactToDelete(contact);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (contactToDelete) {
      deleteContactMutation.mutate(contactToDelete.id, {
        onSuccess: () => {
          setDeleteDialogOpen(false);
          setContactToDelete(null);
        },
      });
    }
  };

  const handleExport = () => {
    exportContactsMutation.mutate();
  };

  const handleMerge = () => {
    if (selectedContacts.length < 2) return;
    setMergeModalOpen(true);
  };

  const selectedContactObjects = contacts.filter(c => selectedContacts.includes(c.id));

  if (isLoading) {
    return <LoadingSpinner message="Loading contacts..." />;
  }

  return (
    <>
      <PageHeader
        title="Contacts List"
        subtitle="View and manage all your contacts"
        actions={
          <Box display="flex" gap={1}>
            <Button
              variant="outlined"
              startIcon={<FileUpload />}
              onClick={() => setImportModalOpen(true)}
            >
              Import
            </Button>
            <Button
              variant="outlined"
              startIcon={<FileDownload />}
              onClick={handleExport}
              loading={exportContactsMutation.isPending}
            >
              Export
            </Button>
            <Button
              variant="contained"
              startIcon={<Add />}
              onClick={() => setContactFormOpen(true)}
            >
              Add Contact
            </Button>
          </Box>
        }
      />
      
      {/* Filters */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" gap={2} flexWrap="wrap" alignItems="center">
          <TextField
            size="small"
            placeholder="Search contacts..."
            value={filters.search || ''}
            onChange={(e) => handleFilterChange('search', e.target.value)}
            sx={{ minWidth: 200 }}
          />
          
          <TextField
            select
            size="small"
            label="Group"
            value={filters.group || ''}
            onChange={(e) => handleFilterChange('group', e.target.value)}
            sx={{ minWidth: 150 }}
          >
            <MenuItem value="">All Groups</MenuItem>
            {groups.map((group) => (
              <MenuItem key={group.id} value={group.id}>
                <Box display="flex" alignItems="center" gap={1}>
                  <Box
                    sx={{
                      width: 12,
                      height: 12,
                      backgroundColor: group.color,
                      borderRadius: '50%',
                    }}
                  />
                  {group.name}
                </Box>
              </MenuItem>
            ))}
          </TextField>
          
          <TextField
            select
            size="small"
            label="Status"
            value={filters.is_active === undefined ? '' : filters.is_active.toString()}
            onChange={(e) => handleFilterChange('is_active', e.target.value === '' ? undefined : e.target.value === 'true')}
            sx={{ minWidth: 120 }}
          >
            <MenuItem value="">All</MenuItem>
            <MenuItem value="true">Active</MenuItem>
            <MenuItem value="false">Inactive</MenuItem>
          </TextField>
          
          {selectedContacts.length > 0 && (
            <Box display="flex" gap={1} alignItems="center">
              <Typography variant="body2">
                {selectedContacts.length} selected
              </Typography>
              <Button
                size="small"
                variant="outlined"
                startIcon={<MergeType />}
                onClick={handleMerge}
                disabled={selectedContacts.length < 2}
              >
                Merge
              </Button>
            </Box>
          )}
        </Box>
      </Paper>

      {/* Contacts Table */}
      <Paper>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell padding="checkbox">
                  <Checkbox
                    checked={selectedContacts.length === contacts.length && contacts.length > 0}
                    indeterminate={selectedContacts.length > 0 && selectedContacts.length < contacts.length}
                    onChange={handleSelectAll}
                  />
                </TableCell>
                <TableCell>Contact</TableCell>
                <TableCell>Company</TableCell>
                <TableCell>Tags</TableCell>
                <TableCell>Bookings</TableCell>
                <TableCell>Last Booking</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {contacts.map((contact, index) => (
                <motion.tr
                  key={contact.id}
                  component={TableRow}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.2, delay: index * 0.05 }}
                  hover
                >
                  <TableCell padding="checkbox">
                    <Checkbox
                      checked={selectedContacts.includes(contact.id)}
                      onChange={() => handleSelectContact(contact.id)}
                    />
                  </TableCell>
                  
                  <TableCell>
                    <Box display="flex" alignItems="center" gap={2}>
                      <Avatar sx={{ width: 32, height: 32 }}>
                        {(contact.first_name || '').charAt(0)}
                      </Avatar>
                      <Box>
                        <Typography variant="subtitle2" fontWeight={600}>
                          {contact.full_name}
                        </Typography>
                        <Box display="flex" alignItems="center" gap={1}>
                          <Email fontSize="small" color="action" />
                          <Typography variant="caption" color="text.secondary">
                            {contact.email}
                          </Typography>
                        </Box>
                        {contact.phone && (
                          <Box display="flex" alignItems="center" gap={1}>
                            <Phone fontSize="small" color="action" />
                            <Typography variant="caption" color="text.secondary">
                              {contact.phone}
                            </Typography>
                          </Box>
                        )}
                      </Box>
                    </Box>
                  </TableCell>
                  
                  <TableCell>
                    {contact.company && (
                      <Box display="flex" alignItems="center" gap={1}>
                        <Business fontSize="small" color="action" />
                        <Typography variant="body2">
                          {contact.company}
                        </Typography>
                      </Box>
                    )}
                    {contact.job_title && (
                      <Typography variant="caption" color="text.secondary">
                        {contact.job_title}
                      </Typography>
                    )}
                  </TableCell>
                  
                  <TableCell>
                    <Box display="flex" gap={0.5} flexWrap="wrap">
                      {contact.tags.slice(0, 3).map((tag) => (
                        <Chip
                          key={tag}
                          label={tag}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                      {contact.tags.length > 3 && (
                        <Chip
                          label={`+${contact.tags.length - 3}`}
                          size="small"
                          variant="outlined"
                          color="primary"
                        />
                      )}
                    </Box>
                  </TableCell>
                  
                  <TableCell>
                    <Typography variant="body2" fontWeight={600}>
                      {contact.total_bookings}
                    </Typography>
                  </TableCell>
                  
                  <TableCell>
                    {contact.last_booking_date ? (
                      <Typography variant="body2">
                        {formatDate(contact.last_booking_date)}
                      </Typography>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        Never
                      </Typography>
                    )}
                  </TableCell>
                  
                  <TableCell>
                    <Chip
                      label={contact.is_active ? 'Active' : 'Inactive'}
                      size="small"
                      color={contact.is_active ? 'success' : 'default'}
                      variant={contact.is_active ? 'filled' : 'outlined'}
                    />
                  </TableCell>
                  
                  <TableCell>
                    <Box display="flex" gap={0.5}>
                      <Tooltip title="View Details">
                        <IconButton
                          size="small"
                          onClick={() => navigate(`/contacts/detail/${contact.id}`)}
                        >
                          <Visibility />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Edit">
                        <IconButton
                          size="small"
                          onClick={() => {
                            // TODO: Implement edit functionality
                          }}
                        >
                          <Edit />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton
                          size="small"
                          onClick={() => handleDeleteContact(contact)}
                        >
                          <Delete />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </motion.tr>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        
        <TablePagination
          component="div"
          count={totalContacts}
          page={page}
          onPageChange={(_, newPage) => setPage(newPage)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          rowsPerPageOptions={[10, 20, 50, 100]}
        />
      </Paper>

      {/* Contact Form Modal */}
      <ContactForm
        open={contactFormOpen}
        onClose={() => setContactFormOpen(false)}
        onSubmit={(data) => {
          createContactMutation.mutate(data, {
            onSuccess: () => {
              setContactFormOpen(false);
            },
          });
        }}
        loading={createContactMutation.isPending}
      />

      {/* Import Modal */}
      <ContactImportModal
        open={importModalOpen}
        onClose={() => setImportModalOpen(false)}
        onSubmit={() => {
          // TODO: Implement import
          setImportModalOpen(false);
        }}
      />

      {/* Merge Modal */}
      <ContactMergeModal
        open={mergeModalOpen}
        onClose={() => setMergeModalOpen(false)}
        contacts={selectedContactObjects}
        onSubmit={(data) => {
          mergeContactsMutation.mutate(data, {
            onSuccess: () => {
              setMergeModalOpen(false);
              setSelectedContacts([]);
            },
          });
        }}
        loading={mergeContactsMutation.isPending}
      />

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle>Delete Contact</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete {contactToDelete?.full_name}? This action cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} variant="outlined">
            Cancel
          </Button>
          <Button
            onClick={confirmDelete}
            variant="contained"
            color="error"
            loading={deleteContactMutation.isPending}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default ContactsList;