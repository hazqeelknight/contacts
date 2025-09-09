import React from 'react';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Avatar,
  IconButton,
  Tooltip,
  Chip,
  Menu,
  MenuItem,
} from '@mui/material';
import {
  Edit,
  Delete,
  PersonAdd,
  PersonRemove,
  Email,
  Phone,
  Business,
  MoreVert,
} from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useParams, useNavigate } from 'react-router-dom';
import { PageHeader, LoadingSpinner, Button } from '@/components/core';
import {
  useContactGroup,
  useDeleteContactGroup,
  useContacts,
  useAddContactToGroup,
  useRemoveContactFromGroup,
} from '../api';
import { Contact } from '../types';

const ContactGroupDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [addContactMenuAnchor, setAddContactMenuAnchor] = React.useState<null | HTMLElement>(null);

  const { data: group, isLoading: groupLoading } = useContactGroup(id!);
  const { data: allContactsData } = useContacts();
  const deleteGroupMutation = useDeleteContactGroup();
  const addToGroupMutation = useAddContactToGroup();
  const removeFromGroupMutation = useRemoveContactFromGroup();

  const allContacts = allContactsData?.results || [];
  const groupContacts = group?.contacts || [];
  const availableContacts = allContacts.filter(
    contact => !groupContacts.some(gc => gc.id === contact.id)
  );

  const handleDelete = () => {
    if (group) {
      deleteGroupMutation.mutate(group.id, {
        onSuccess: () => {
          navigate('/contacts/groups');
        },
      });
    }
  };

  const handleAddContact = (contactId: string) => {
    if (group) {
      addToGroupMutation.mutate({ contactId, groupId: group.id });
    }
    setAddContactMenuAnchor(null);
  };

  const handleRemoveContact = (contactId: string) => {
    if (group) {
      removeFromGroupMutation.mutate({ contactId, groupId: group.id });
    }
  };

  if (groupLoading) {
    return <LoadingSpinner message="Loading group details..." />;
  }

  if (!group) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <Typography variant="h6" color="text.secondary">
          Group not found
        </Typography>
      </Box>
    );
  }

  return (
    <>
      <PageHeader
        title={group.name}
        subtitle={`${group.contact_count} contact${group.contact_count !== 1 ? 's' : ''} in this group`}
        breadcrumbs={[
          { label: 'Contacts', href: '/contacts' },
          { label: 'Groups', href: '/contacts/groups' },
          { label: group.name },
        ]}
        actions={
          <Box display="flex" gap={1}>
            <Button
              variant="outlined"
              startIcon={<PersonAdd />}
              onClick={(e) => setAddContactMenuAnchor(e.currentTarget)}
              disabled={availableContacts.length === 0}
            >
              Add Contact
            </Button>
            <Button
              variant="outlined"
              startIcon={<Edit />}
              onClick={() => {
                // TODO: Implement edit functionality
              }}
            >
              Edit Group
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<Delete />}
              onClick={handleDelete}
              loading={deleteGroupMutation.isPending}
            >
              Delete Group
            </Button>
          </Box>
        }
      />

      <Grid container spacing={3}>
        {/* Group Information */}
        <Grid item xs={12} md={4}>
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.3 }}
          >
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2} mb={3}>
                  <Box
                    sx={{
                      width: 48,
                      height: 48,
                      backgroundColor: group.color,
                      borderRadius: 2,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                    }}
                  >
                    <Typography variant="h6" color="white" fontWeight={600}>
                      {group.name.charAt(0).toUpperCase()}
                    </Typography>
                  </Box>
                  <Box>
                    <Typography variant="h5" fontWeight={600}>
                      {group.name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Created {new Date(group.created_at).toLocaleDateString()}
                    </Typography>
                  </Box>
                </Box>

                {group.description && (
                  <>
                    <Typography variant="subtitle2" gutterBottom>
                      Description
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                      {group.description}
                    </Typography>
                  </>
                )}

                <Box display="flex" alignItems="center" gap={1}>
                  <Typography variant="subtitle2">Color:</Typography>
                  <Box
                    sx={{
                      width: 20,
                      height: 20,
                      backgroundColor: group.color,
                      borderRadius: 1,
                      border: '1px solid #ccc',
                    }}
                  />
                  <Typography variant="body2" color="text.secondary">
                    {group.color}
                  </Typography>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Group Contacts */}
        <Grid item xs={12} md={8}>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: 0.1 }}
          >
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Contacts in this Group
                </Typography>

                {groupContacts.length > 0 ? (
                  <List>
                    {groupContacts.map((contact, index) => (
                      <ListItem
                        key={contact.id}
                        sx={{
                          border: '1px solid',
                          borderColor: 'divider',
                          borderRadius: 1,
                          mb: 1,
                          '&:hover': {
                            backgroundColor: 'action.hover',
                          },
                        }}
                        secondaryAction={
                          <Box display="flex" gap={0.5}>
                            <Tooltip title="View Contact">
                              <IconButton
                                size="small"
                                onClick={() => navigate(`/contacts/detail/${contact.id}`)}
                              >
                                <MoreVert />
                              </IconButton>
                            </Tooltip>
                            <Tooltip title="Remove from Group">
                              <IconButton
                                size="small"
                                onClick={() => handleRemoveContact(contact.id)}
                              >
                                <PersonRemove />
                              </IconButton>
                            </Tooltip>
                          </Box>
                        }
                      >
                        <ListItemAvatar>
                          <Avatar>
                            {contact.first_name.charAt(0)}
                          </Avatar>
                        </ListItemAvatar>
                        <ListItemText
                          disableTypography
                          primary={
                            <Box display="flex" alignItems="center" gap={1}>
                              <Typography variant="subtitle2" fontWeight={600}>
                                {contact.full_name}
                              </Typography>
                              <Chip
                                label={contact.is_active ? 'Active' : 'Inactive'}
                                size="small"
                                color={contact.is_active ? 'success' : 'default'}
                                variant="outlined"
                              />
                            </Box>
                          }
                          secondary={
                            <Box>
                              <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                                <Email fontSize="small" />
                                <Typography variant="body2">
                                  {contact.email}
                                </Typography>
                              </Box>
                              {contact.phone && (
                                <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                                  <Phone fontSize="small" />
                                  <Typography variant="body2">
                                    {contact.phone}
                                  </Typography>
                                </Box>
                              )}
                              {contact.company && (
                                <Box display="flex" alignItems="center" gap={1}>
                                  <Business fontSize="small" />
                                  <Typography variant="body2">
                                    {contact.company}
                                  </Typography>
                                </Box>
                              )}
                            </Box>
                          }
                        />
                      </ListItem>
                    ))}
                  </List>
                ) : (
                  <Box textAlign="center" py={4}>
                    <Typography variant="body2" color="text.secondary">
                      No contacts in this group yet
                    </Typography>
                  </Box>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>
      </Grid>

      {/* Add Contact Menu */}
      <Menu
        anchorEl={addContactMenuAnchor}
        open={Boolean(addContactMenuAnchor)}
        onClose={() => setAddContactMenuAnchor(null)}
        PaperProps={{
          sx: { maxHeight: 300, width: 300 },
        }}
      >
        {availableContacts.length > 0 ? (
          availableContacts.map((contact) => (
            <MenuItem
              key={contact.id}
              onClick={() => handleAddContact(contact.id)}
            >
              <Box display="flex" alignItems="center" gap={2} width="100%">
                <Avatar sx={{ width: 32, height: 32 }}>
                  {(contact.first_name || '').charAt(0)}
                </Avatar>
                <Box>
                  <Typography variant="subtitle2">
                    {contact.full_name}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {contact.email}
                  </Typography>
                </Box>
              </Box>
            </MenuItem>
          ))
        ) : (
          <MenuItem disabled>
            <Typography variant="body2" color="text.secondary">
              All contacts are already in this group
            </Typography>
          </MenuItem>
        )}
      </Menu>
    </>
  );
};

export default ContactGroupDetail;