import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Box,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Avatar,
  Radio,
  RadioGroup,
  FormControlLabel,
  Alert,
  Divider,
} from '@mui/material';
import { Person, Email, Phone, Business } from '@mui/icons-material';
import { Button } from '@/components/core';
import { Contact } from '../types';

interface ContactMergeModalProps {
  open: boolean;
  onClose: () => void;
  contacts: Contact[];
  onSubmit: (data: { primary_contact_id: string; duplicate_contact_ids: string[] }) => void;
  loading?: boolean;
}

export const ContactMergeModal: React.FC<ContactMergeModalProps> = ({
  open,
  onClose,
  contacts,
  onSubmit,
  loading = false,
}) => {
  const [primaryContactId, setPrimaryContactId] = React.useState<string>('');

  React.useEffect(() => {
    if (contacts.length > 0 && !primaryContactId) {
      // Default to the contact with the most bookings
      const contactWithMostBookings = contacts.reduce((prev, current) => 
        current.total_bookings > prev.total_bookings ? current : prev
      );
      setPrimaryContactId(contactWithMostBookings.id);
    }
  }, [contacts, primaryContactId]);

  const handleSubmit = () => {
    if (!primaryContactId) return;
    
    const duplicateIds = contacts
      .filter(contact => contact.id !== primaryContactId)
      .map(contact => contact.id);
    
    onSubmit({
      primary_contact_id: primaryContactId,
      duplicate_contact_ids: duplicateIds,
    });
  };

  const handleClose = () => {
    setPrimaryContactId('');
    onClose();
  };

  if (contacts.length < 2) {
    return (
      <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
        <DialogTitle>Merge Contacts</DialogTitle>
        <DialogContent>
          <Alert severity="warning">
            Please select at least 2 contacts to merge.
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} variant="outlined">
            Close
          </Button>
        </DialogActions>
      </Dialog>
    );
  }

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>Merge Contacts</DialogTitle>
      
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          <Alert severity="info" sx={{ mb: 3 }}>
            Select which contact should be kept as the primary contact. All data from other contacts will be merged into the primary contact, and the duplicate contacts will be deleted.
          </Alert>

          <Typography variant="h6" gutterBottom>
            Select Primary Contact:
          </Typography>

          <RadioGroup
            value={primaryContactId}
            onChange={(e) => setPrimaryContactId(e.target.value)}
          >
            <List>
              {contacts.map((contact, index) => (
                <React.Fragment key={contact.id}>
                  <ListItem
                    sx={{
                      border: '1px solid',
                      borderColor: primaryContactId === contact.id ? 'primary.main' : 'divider',
                      borderRadius: 1,
                      mb: 1,
                      backgroundColor: primaryContactId === contact.id ? 'action.selected' : 'background.paper',
                    }}
                  >
                    <FormControlLabel
                      value={contact.id}
                      control={<Radio />}
                      label=""
                      sx={{ mr: 2 }}
                    />
                    
                    <ListItemAvatar>
                      <Avatar>
                        {contact.first_name ? contact.first_name.charAt(0) : <Person />}
                      </Avatar>
                    </ListItemAvatar>
                    
                    <ListItemText
                      primary={
                        <Box display="flex" alignItems="center" gap={1}>
                          <Typography variant="subtitle1" fontWeight={600}>
                            {contact.full_name}
                          </Typography>
                          {primaryContactId === contact.id && (
                            <Typography variant="caption" color="primary" fontWeight={600}>
                              (Primary)
                            </Typography>
                          )}
                        </Box>
                      }
                      secondary={
                        <Box>
                          <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                            <Email fontSize="small" />
                            <Typography variant="body2">{contact.email}</Typography>
                          </Box>
                          {contact.phone && (
                            <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                              <Phone fontSize="small" />
                              <Typography variant="body2">{contact.phone}</Typography>
                            </Box>
                          )}
                          {contact.company && (
                            <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                              <Business fontSize="small" />
                              <Typography variant="body2">{contact.company}</Typography>
                            </Box>
                          )}
                          <Typography variant="caption" color="text.secondary">
                            {contact.total_bookings} booking{contact.total_bookings !== 1 ? 's' : ''} • 
                            Created {new Date(contact.created_at).toLocaleDateString()}
                          </Typography>
                        </Box>
                      }
                    />
                  </ListItem>
                  {index < contacts.length - 1 && <Divider sx={{ my: 1 }} />}
                </React.Fragment>
              ))}
            </List>
          </RadioGroup>

          <Alert severity="warning" sx={{ mt: 3 }}>
            <Typography variant="subtitle2" gutterBottom>
              This action cannot be undone!
            </Typography>
            <Typography variant="body2">
              The selected primary contact will retain all its information, plus:
              • All interactions from duplicate contacts
              • Combined booking statistics
              • Merged tags and notes
              • All group memberships
            </Typography>
          </Alert>
        </Box>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={handleClose} variant="outlined">
          Cancel
        </Button>
        <Button
          onClick={handleSubmit}
          variant="contained"
          color="warning"
          loading={loading}
          disabled={!primaryContactId}
        >
          Merge Contacts
        </Button>
      </DialogActions>
    </Dialog>
  );
};