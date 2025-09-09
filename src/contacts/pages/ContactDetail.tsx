import React from 'react';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  Chip,
  Avatar,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Divider,
  IconButton,
  Tooltip,
  Menu,
  MenuItem,
} from '@mui/material';
import {
  Email,
  Phone,
  Business,
  Person,
  Edit,
  Delete,
  Add,
  MoreVert,
  Group,
  History,
  Event,
} from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useParams, useNavigate } from 'react-router-dom';
import { PageHeader, LoadingSpinner, Button } from '@/components/core';
import {
  useContact,
  useContactInteractions,
  useDeleteContact,
  useContactGroups,
  useAddContactToGroup,
  useRemoveContactFromGroup,
} from '../api';
import { AddInteractionModal } from '../components/AddInteractionModal';
import { formatDate, formatRelativeTime } from '@/utils/formatters';

const ContactDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [interactionModalOpen, setInteractionModalOpen] = React.useState(false);
  const [groupMenuAnchor, setGroupMenuAnchor] = React.useState<null | HTMLElement>(null);

  const { data: contact, isLoading: contactLoading } = useContact(id!);
  const { data: interactionsData, isLoading: interactionsLoading } = useContactInteractions(id);
  const { data: groupsData } = useContactGroups();
  const deleteContactMutation = useDeleteContact();
  const addToGroupMutation = useAddContactToGroup();
  const removeFromGroupMutation = useRemoveContactFromGroup();

  const interactions = interactionsData?.results || [];
  const allGroups = groupsData?.results || [];
  const contactGroups = contact?.groups || [];
  const availableGroups = allGroups.filter(
    group => !contactGroups.some(cg => cg.id === group.id)
  );

  const handleDelete = () => {
    if (contact) {
      deleteContactMutation.mutate(contact.id, {
        onSuccess: () => {
          navigate('/contacts/list');
        },
      });
    }
  };

  const handleAddToGroup = (groupId: string) => {
    if (contact) {
      addToGroupMutation.mutate({ contactId: contact.id, groupId });
    }
    setGroupMenuAnchor(null);
  };

  const handleRemoveFromGroup = (groupId: string) => {
    if (contact) {
      removeFromGroupMutation.mutate({ contactId: contact.id, groupId });
    }
  };

  if (contactLoading) {
    return <LoadingSpinner message="Loading contact details..." />;
  }

  if (!contact) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <Typography variant="h6" color="text.secondary">
          Contact not found
        </Typography>
      </Box>
    );
  }

  return (
    <>
      <PageHeader
        title={contact.full_name}
        subtitle={`Contact details and interaction history`}
        breadcrumbs={[
          { label: 'Contacts', href: '/contacts' },
          { label: 'List', href: '/contacts/list' },
          { label: contact.full_name },
        ]}
        actions={
          <Box display="flex" gap={1}>
            <Button
              variant="outlined"
              startIcon={<Edit />}
              onClick={() => {
                // TODO: Implement edit functionality
              }}
            >
              Edit
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<Delete />}
              onClick={handleDelete}
              loading={deleteContactMutation.isPending}
            >
              Delete
            </Button>
          </Box>
        }
      />

      <Grid container spacing={3}>
        {/* Contact Information */}
        <Grid item xs={12} md={4}>
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.3 }}
          >
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2} mb={3}>
                  <Avatar sx={{ width: 64, height: 64, fontSize: '1.5rem' }}>
                    {(contact.first_name || '').charAt(0)}
                  </Avatar>
                  <Box>
                    <Typography variant="h5" fontWeight={600}>
                      {contact.full_name}
                    </Typography>
                    <Chip
                      label={contact.is_active ? 'Active' : 'Inactive'}
                      size="small"
                      color={contact.is_active ? 'success' : 'default'}
                      variant={contact.is_active ? 'filled' : 'outlined'}
                    />
                  </Box>
                </Box>

                <List dense>
                  <ListItem sx={{ px: 0 }}>
                    <ListItemIcon>
                      <Email color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary="Email"
                      secondary={contact.email}
                    />
                  </ListItem>

                  {contact.phone && (
                    <ListItem sx={{ px: 0 }}>
                      <ListItemIcon>
                        <Phone color="primary" />
                      </ListItemIcon>
                      <ListItemText
                        primary="Phone"
                        secondary={contact.phone}
                      />
                    </ListItem>
                  )}

                  {contact.company && (
                    <ListItem sx={{ px: 0 }}>
                      <ListItemIcon>
                        <Business color="primary" />
                      </ListItemIcon>
                      <ListItemText
                        primary="Company"
                        secondary={contact.company}
                      />
                    </ListItem>
                  )}

                  {contact.job_title && (
                    <ListItem sx={{ px: 0 }}>
                      <ListItemIcon>
                        <Person color="primary" />
                      </ListItemIcon>
                      <ListItemText
                        primary="Job Title"
                        secondary={contact.job_title}
                      />
                    </ListItem>
                  )}

                  <ListItem sx={{ px: 0 }}>
                    <ListItemIcon>
                      <Event color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary="Total Bookings"
                      secondary={contact.total_bookings}
                    />
                  </ListItem>

                  {contact.last_booking_date && (
                    <ListItem sx={{ px: 0 }}>
                      <ListItemIcon>
                        <History color="primary" />
                      </ListItemIcon>
                      <ListItemText
                        primary="Last Booking"
                        secondary={formatDate(contact.last_booking_date)}
                      />
                    </ListItem>
                  )}
                </List>

                {contact.tags.length > 0 && (
                  <>
                    <Divider sx={{ my: 2 }} />
                    <Typography variant="subtitle2" gutterBottom>
                      Tags
                    </Typography>
                    <Box display="flex" gap={0.5} flexWrap="wrap">
                      {contact.tags.map((tag) => (
                        <Chip
                          key={tag}
                          label={tag}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </>
                )}

                {contact.notes && (
                  <>
                    <Divider sx={{ my: 2 }} />
                    <Typography variant="subtitle2" gutterBottom>
                      Notes
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {contact.notes}
                    </Typography>
                  </>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Groups and Interactions */}
        <Grid item xs={12} md={8}>
          <Grid container spacing={3}>
            {/* Contact Groups */}
            <Grid item xs={12}>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3, delay: 0.1 }}
              >
                <Card>
                  <CardContent>
                    <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
                      <Box display="flex" alignItems="center" gap={1}>
                        <Group color="primary" />
                        <Typography variant="h6">Groups</Typography>
                      </Box>
                      <Box>
                        <IconButton
                          onClick={(e) => setGroupMenuAnchor(e.currentTarget)}
                          disabled={availableGroups.length === 0}
                        >
                          <MoreVert />
                        </IconButton>
                        <Menu
                          anchorEl={groupMenuAnchor}
                          open={Boolean(groupMenuAnchor)}
                          onClose={() => setGroupMenuAnchor(null)}
                        >
                          {availableGroups.map((group) => (
                            <MenuItem
                              key={group.id}
                              onClick={() => handleAddToGroup(group.id)}
                            >
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
                        </Menu>
                      </Box>
                    </Box>

                    {contactGroups.length > 0 ? (
                      <Box display="flex" gap={1} flexWrap="wrap">
                        {contactGroups.map((group) => (
                          <Chip
                            key={group.id}
                            label={group.name}
                            onDelete={() => handleRemoveFromGroup(group.id)}
                            sx={{
                              backgroundColor: group.color + '20',
                              borderColor: group.color,
                              color: group.color,
                            }}
                            variant="outlined"
                          />
                        ))}
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        This contact is not in any groups
                      </Typography>
                    )}
                  </CardContent>
                </Card>
              </motion.div>
            </Grid>

            {/* Interactions */}
            <Grid item xs={12}>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3, delay: 0.2 }}
              >
                <Card>
                  <CardContent>
                    <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
                      <Box display="flex" alignItems="center" gap={1}>
                        <History color="primary" />
                        <Typography variant="h6">Interaction History</Typography>
                      </Box>
                      <Button
                        variant="outlined"
                        size="small"
                        startIcon={<Add />}
                        onClick={() => setInteractionModalOpen(true)}
                      >
                        Add Interaction
                      </Button>
                    </Box>

                    {interactionsLoading ? (
                      <LoadingSpinner message="Loading interactions..." />
                    ) : interactions.length > 0 ? (
                      <List>
                        {interactions.map((interaction, index) => (
                          <React.Fragment key={interaction.id}>
                            <ListItem sx={{ px: 0, alignItems: 'flex-start' }}>
                              <ListItemText
                                primary={
                                  <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="subtitle2">
                                      {interaction.interaction_type_display}
                                    </Typography>
                                    {interaction.booking_id && (
                                      <Chip
                                        label="Booking"
                                        size="small"
                                        color="primary"
                                        variant="outlined"
                                        onClick={() => navigate(`/events/bookings/${interaction.booking_id}`)}
                                        sx={{ cursor: 'pointer' }}
                                      />
                                    )}
                                  </Box>
                                }
                                secondary={
                                  <Box>
                                    <Typography variant="body2" sx={{ mb: 0.5 }}>
                                      {interaction.description}
                                    </Typography>
                                    <Typography variant="caption" color="text.secondary">
                                      {formatRelativeTime(interaction.created_at)}
                                    </Typography>
                                  </Box>
                                }
                              />
                            </ListItem>
                            {index < interactions.length - 1 && <Divider />}
                          </React.Fragment>
                        ))}
                      </List>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        No interactions recorded yet
                      </Typography>
                    )}
                  </CardContent>
                </Card>
              </motion.div>
            </Grid>
          </Grid>
        </Grid>
      </Grid>

      {/* Add Interaction Modal */}
      <AddInteractionModal
        open={interactionModalOpen}
        onClose={() => setInteractionModalOpen(false)}
        contactName={contact.full_name}
        onSubmit={() => {
          // TODO: Implement add interaction
          setInteractionModalOpen(false);
        }}
      />
    </>
  );
};

export default ContactDetail;